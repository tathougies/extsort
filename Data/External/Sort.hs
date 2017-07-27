module Data.External.Sort where

import           Prelude hiding (log, read, pred)

import           Data.External.Tape

import           Control.Applicative ((<|>))
import           Control.DeepSeq (NFData)
import           Control.Monad (when, unless, forM_, forM, join)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Trans.Resource (MonadResource, allocate)

import qualified Data.Attoparsec.Binary as Atto (word32be)
import           Data.Attoparsec.ByteString.Char8 as Atto hiding (take)
import qualified Data.ByteString.Builder as B
import           Data.Conduit ((=$=), Conduit, ConduitM, Producer, await, yield)
import           Data.Function (on)
import           Data.IORef (readIORef, writeIORef)
import           Data.List (genericReplicate, tails, minimumBy)
import           Data.Monoid ((<>))
import qualified Data.Mutable as Mut (IOPRef, newRef, readRef, writeRef, modifyRef')
import           Data.Ord (comparing)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as VIO (sortByBounds)
import qualified Data.Vector.Algorithms.Heap as VIO (heapify, pop, heapInsert)
import qualified Data.Vector.Mutable as VIO
import           Data.Word (Word8, Word32)

import           Foreign.C.Types

import           GHC.Generics
import           GHC.Stack

import           System.IO
import           System.Posix

data SortingEvent k
    = SortingEventFinished
    | SortingEventRunDone
    | SortingEventKey k
      deriving (Show, Generic)
instance NFData k => NFData (SortingEvent k)

data Serializers a
    = Serializers
    { serialParser  :: Atto.Parser a
    , serialBuilder :: a -> B.Builder
    }

data SortOptions
    = SortOptions
    { sortOptionsWorkingMem  :: CSize -- ^ working memory, in kilobytes
    , sortOptionsTapes       :: Word8 -- ^ Number of tapes to use

    , sortOptionsChunkSize   :: Int -- ^ Number to include in initial runs

    , sortOptionsWorkingFile :: FilePath -- ^ Where to store tapes
    , sortOptionsPersistFile :: Bool     -- ^ If True, the working file will not be deleted after the sort
    } deriving Show

-- * Constants

kMIN_BLOCK_SIZE :: CSize
kMIN_BLOCK_SIZE = 8192

kBLOCK_ALIGNMENT :: CSize
kBLOCK_ALIGNMENT = 4096

runMarkerWord, finishedMarkerWord, dataMarkerWord :: Word32
runMarkerWord = 0x0a0a0a0a
finishedMarkerWord = 0xF1F1F1F1
dataMarkerWord = 0xD0D0D0D0

runMarker, finishedMarker, dataMarker :: B.Builder
runMarker      = B.word32BE runMarkerWord
finishedMarker = B.word32BE finishedMarkerWord
dataMarker     = B.word32BE dataMarkerWord

-- * Utilities

isSortingFinished :: SortingEvent k -> Bool
isSortingFinished SortingEventFinished = True
isSortingFinished _ = False

compareSortingEvent' :: (k -> k -> Ordering)
                     -> SortingEvent k -> SortingEvent k -> Ordering
compareSortingEvent' _ SortingEventFinished SortingEventFinished = EQ
compareSortingEvent' _ SortingEventFinished _  = LT
compareSortingEvent' _ _ SortingEventFinished = GT
compareSortingEvent' _ SortingEventRunDone SortingEventRunDone = EQ
compareSortingEvent' _ SortingEventRunDone _ = LT
compareSortingEvent' _ _ SortingEventRunDone = GT
compareSortingEvent' cmp (SortingEventKey a) (SortingEventKey b) = cmp a b

compareSortingEvent :: (k -> k -> Ordering)
                    -> SortingEvent k -> SortingEvent k -> Ordering
compareSortingEvent cmp a b =
  flipOrd (compareSortingEvent' (\a' b' -> cmp a' b') a b)
  where
    flipOrd :: Ordering -> Ordering
    flipOrd EQ = EQ
    flipOrd LT = GT
    flipOrd GT = LT

removeAt :: Int -> V.Vector a -> V.Vector a
removeAt ix = do
  (before, after) <- V.splitAt ix
  if V.null after then pure before else pure (before <> V.tail after)

debugMVector :: VIO.IOVector a -> (V.Vector a -> IO ()) -> IO ()
debugMVector _ _ = pure ()

log, info:: MonadIO m => String -> m ()
--log = liftIO . hPutStrLn stderr
log _ = pure ()

info = liftIO . hPutStrLn stderr

-- | Compute the kth order fibonacci
kthFibonacci :: (Integral a, Num a) => Int -> [(a, [a])]
kthFibonacci n =
    let fibInit 0 = []
        fibInit 1 = 1:map (sum . take n) (tails x)
        fibInit n' = 0:fibInit (n' - 1)

        x = fibInit n
    in map (\x' -> let ns = take n x' in ( sum ns, ns )) (tails x)

-- * External sort

externalSort :: ( MonadResource m, MonadIO m, NFData k, NFData v, Show k )
             => SortOptions
             -> (k -> k -> Ordering)
             -> Serializers k
             -> Serializers v
             -> Copier v
             -> Conduit (k, v) m (k, v)
externalSort options cmpKey keySerializers valueSerializers valueCopier =
  do let unalignedBlockSize = (sortOptionsWorkingMem options * 1024) `div`
                              fromIntegral (sortOptionsTapes options)
         blockSize = ((unalignedBlockSize + kBLOCK_ALIGNMENT - 1) `div` kBLOCK_ALIGNMENT) *
                     kBLOCK_ALIGNMENT

     when (blockSize <= kMIN_BLOCK_SIZE) (fail "externalSort: Not enough space")

     (_, tapeFile) <- allocate (openTapeFile (sortOptionsWorkingFile options) blockSize) closeTapeFile
     unless (sortOptionsPersistFile options) $
       liftIO (removeLink (sortOptionsWorkingFile options))

     info "[SORT] Reading input"
     tapes <- sortChunks cmpKey (sortOptionsChunkSize options) =$=
              sinkInitialTapes options keySerializers valueSerializers tapeFile

     info "[SORT] [MERGE] Starting..."
     outputTape <-
         liftIO $ do
           forM_ (V.tail tapes) rewindTape
           nwayMerges (V.tail tapes) (V.head tapes) cmpKey keySerializers valueCopier
     info "[SORT] [MERGE] Done..."

     sourceAllRawRows keySerializers valueSerializers outputTape

     info "[SORT] Done"

sortChunks :: MonadIO m => (k -> k -> Ordering) -> Int
           -> Conduit (k, v) m (V.Vector (k, v))
sortChunks cmpKey chunkSz = do
  v <- liftIO (VIO.new chunkSz)

  tuplesRead <- read 0 v

  v'' <- liftIO $ do
           VIO.sortByBounds (cmpKey `on` fst) v 0 tuplesRead
           let v' = VIO.slice 0 tuplesRead v
           V.unsafeFreeze v'

  yield v''

  if tuplesRead < chunkSz
     then pure ()
     else sortChunks cmpKey chunkSz

  where
    read curIdx v
      | curIdx == chunkSz = pure curIdx
      | otherwise = do
          x <- await
          case x of
            Nothing -> pure curIdx
            Just row -> do
              liftIO (VIO.write v curIdx row)
              read (curIdx + 1) v

-- | Read all rows
sourceAllRawRows :: (MonadIO m, NFData v, NFData k)
                 => Serializers k -> Serializers v
                 -> Tape -> Producer m (k, v)
sourceAllRawRows keySerializers valueSerializers tp =
  do res <- liftIO $ parseFromTape rawRowParser tp
     case res of
       Left (ctxt, err) -> fail ("sourceAllRows: no parse: " ++ show ctxt ++ ": " ++ err)
       Right SortingEventFinished -> pure ()
       Right SortingEventRunDone  ->
         sourceAllRawRows keySerializers valueSerializers tp
       Right (SortingEventKey kv) ->
         do yield kv
            sourceAllRawRows keySerializers valueSerializers tp
  where
    rawRowParser = do
      event <- sortingEventParser (serialParser keySerializers)
      case event of
        SortingEventKey key ->
          do v <- serialParser valueSerializers
             pure (SortingEventKey (key, v))
        SortingEventRunDone ->
          pure SortingEventRunDone
        SortingEventFinished ->
          pure SortingEventFinished

-- | Distribute initial runs
sinkInitialTapes
    :: ( MonadResource m, MonadIO m )
    => SortOptions
    -> Serializers k
    -> Serializers v
    -> TapeFile
    -> ConduitM (V.Vector (k, v)) o m (V.Vector Tape)
sinkInitialTapes SortOptions { sortOptionsTapes       = tapeCount }
                 keySerializers valueSerializers
                 tapeFile =
  do tapes <- liftIO (V.replicateM (fromIntegral tapeCount) (openTape tapeFile))

     let sizes = kthFibonacci (fromIntegral (tapeCount - 1))

     (totalSz, outputTapeSizes) <- readRuns tapes 0 sizes

     -- Add dummy runs
     liftIO . forM_ (V.zip (V.fromList outputTapeSizes) (V.tail tapes)) $ \(tapeSz, tape) -> do
         recs <- Mut.readRef (tapeRuns tape)

         let dummies = fromIntegral tapeSz - recs
             dummyBuilder = mconcat (genericReplicate dummies runMarker) <>
                            if tapeSz == 0 then runMarker else mempty <>
                            finishedMarker

         writeTapeBuilder tape dummyBuilder
         Mut.writeRef (tapeRuns tape) tapeSz

     info ("Tape sizes " ++ show outputTapeSizes ++ " = " ++ show totalSz)

     pure tapes

  where
    readRuns tapes curSz oldSizes@((maxSz, sizes):sizess) = do
      nextTuples <- await
      case nextTuples of
        Nothing -> pure (maxSz, sizes)
        Just tuples -> do
          let (nextSizes, (maxSz', sizes')) =
                  if curSz == maxSz then (sizess, head sizess) else (oldSizes, (maxSz, sizes))

          (_, outputTape) <-
              liftIO (chooseNextTape tapes sizes')

          let builder = foldMap (\(key, value) -> dataMarker <> serialBuilder keySerializers key <> serialBuilder valueSerializers value) tuples <>
                        runMarker
          liftIO (writeTapeBuilder outputTape builder)

          when (curSz == maxSz) $
            info ("Going to use " ++ show maxSz')

          readRuns tapes (curSz + 1) nextSizes
    readRuns _ _ [] = error "impossible"

    -- Choose tape with the least utilization
    chooseNextTape tapes sizes = do
      (tapeIdx, _) <-
          fmap (minimumBy (comparing snd)) .
          forM (zip [1..] sizes) $ \(i, expSz) -> do
            let tape = tapes V.! i
            curSz <- Mut.readRef (tapeRuns tape)
            pure (i, fromIntegral curSz / (fromIntegral expSz :: Double))

      let outputTape = tapes V.! tapeIdx
      Mut.modifyRef' (tapeRuns outputTape) (+1)

      pure (tapeIdx, outputTape)

-- * N-Way merge step

nwayMerges :: forall k v
            . ( HasCallStack, Show k, NFData k )
           => V.Vector Tape -> Tape
           -> (k -> k -> Ordering)
           -> Serializers k -> Copier v
           -> IO Tape
nwayMerges tapes initialOutputTape cmpKey keySerializer valueCopier = do
  nonEmptyTapes <- V.filterM (fmap (> 0) . Mut.readRef . tapeRuns) tapes

  if V.null nonEmptyTapes
     then pure initialOutputTape
     else do
       inputs <- mapM (readSortingRow keySerializer) tapes
       join (nwayMerges' <$> slicedFromVector (compareSortingEvent cmpKey `on` fst) (V.zip inputs tapes)
                         <*> pure initialOutputTape)

  where
    nwayMerges' :: HasCallStack
                => SlicedVector (SortingEvent k, Tape)
                -> Tape -> IO Tape
    nwayMerges' inputs outputTape = do
      (nextOutputTape, inputs') <- nwayMerge' inputs outputTape
      rewindTape outputTape

      debugMVector (sliced inputs') $ \inputs'Frozen ->
        info ("Frozen inputs " ++ show (fmap fst inputs'Frozen))

      inputs'' <- inplaceFilterSlicedM  (compareSortingEvent cmpKey `on` fst)
                                        ( fmap (> 0) . Mut.readRef . tapeRuns . snd ) inputs'

      debugMVector (sliced inputs'') $ \inputs''Frozen ->
        debugMVector (orig inputs'') $ \origInputs''Frozen ->
          info ("Inputs'' " ++ show (fmap fst inputs''Frozen) ++ " " ++ show (fmap fst origInputs''Frozen))

      if slicedNull inputs''
         then do
           info "No more inputs"
           pure outputTape
         else do
           log "Sort again"
           nextOutputSize <- Mut.readRef (tapeRuns nextOutputTape)
           when (nextOutputSize > 0) (fail ("nextOutputSize (" ++ show nextOutputSize ++ ") > 0"))

           outputHd <- readIORef (tapeReadingBlock nextOutputTape)
           writeIORef (tapeReadingBlock nextOutputTape) Nothing

           case outputHd of
             Nothing -> pure ()
             Just outputHd' -> closeTapeHead outputHd'

           outRow <- readSortingRow keySerializer outputTape
           log ("Out row is " ++ show outRow)
           inputs''' <- slicedPushHeap (compareSortingEvent cmpKey `on` fst) inputs'' (outRow, outputTape)
           log "Push heap"

           nwayMerges' inputs''' nextOutputTape

    nwayMerge' :: HasCallStack
               => SlicedVector (SortingEvent k, Tape) -> Tape
               -> IO (Tape, SlicedVector (SortingEvent k, Tape))
    nwayMerge' inputs outputTape
      | slicedNull inputs = do
          info "Inputs null"
          writeTapeBuilder outputTape (runMarker <> finishedMarker)
          pure (outputTape, inputs)

      | otherwise = do
          debugMVector (sliced inputs) $ \inputsFrozen ->
            log ("Inputs " ++ show (fmap fst inputsFrozen))

          inputs' <- join (mergeRun <$> mkSlicedPair inputs
                                    <*> pure outputTape)
          log "Run merged"
          debugMVector (sliced inputs') $ \inputs'Frozen ->
            log ("Inputs' " ++ show (fmap fst inputs'Frozen))

          Mut.modifyRef' (tapeRuns outputTape) (+1)
          writeTapeBuilder outputTape runMarker

          log "take while start"
          (finished, alive) <- slicedTakeWhile (compareSortingEvent cmpKey `on` fst) (isSortingFinished . fst) inputs'
          log "take while end"

          case finished V.!? 0 of
            Nothing -> do
              info "None finish"
              debugMVector (sliced inputs') $ \inputs'Frozen' ->
                log ("Inputs' frozen after none " ++ show (fmap fst inputs'Frozen'))
              nwayMerge' inputs' outputTape
            Just (SortingEventFinished, nextOutput) -> do
              -- Release all the non-output tapes that are done now
              forM_ finished $ \(s, finishedTp) -> do
                log ("Finish tape head " ++ show s)
                rdHead <- readIORef (tapeReadingBlock finishedTp)
                writeIORef (tapeReadingBlock finishedTp) Nothing

                case rdHead of
                  Nothing -> pure ()
                  Just rdHead' -> closeReadHead (tapeTapeFile finishedTp) rdHead'

              -- We're now done with this tape
              writeTapeBuilder outputTape finishedMarker
              log "wrote finished"

              debugMVector (sliced alive) $ \aliveFrozen ->
                log ("Alive before loop " ++ show (fmap fst aliveFrozen))

              pure (nextOutput, alive)
            Just _ -> fail "Not finished"

    mergeRun :: HasCallStack
             => SlicedVectorPair (SortingEvent k, Tape)
             -> Tape -> IO (SlicedVector (SortingEvent k, Tape))
    mergeRun inputsAndDone outputTape = do
      firstInput <- firstSliceHead inputsAndDone
      case firstInput of
        Nothing -> do
          log "mergeRun done"
          slicedPairTakeSecond (compareSortingEvent cmpKey `on` fst) inputsAndDone
        Just (SortingEventFinished, _) ->
          fail "mergeRun: File finished before run done"
        Just (SortingEventRunDone, tp) -> do
          x' <- readSortingRow keySerializer tp

          slicedMoveHeadToSecondAndSet (compareSortingEvent cmpKey `on` fst) inputsAndDone (x', tp)
          Mut.modifyRef' (tapeRuns tp) (subtract 1)

          mergeRun inputsAndDone outputTape
        Just (SortingEventKey lowestKey, tp) -> do
          writeTapeBuilder outputTape (dataMarker <> serialBuilder keySerializer lowestKey)
          runCopierInTape valueCopier tp outputTape

          x' <- readSortingRow keySerializer tp
          slicedPopHeadAndInsertSortedInFirst (compareSortingEvent cmpKey `on` fst) inputsAndDone (x', tp)

          mergeRun inputsAndDone outputTape

readSortingRow :: forall k. NFData k
               => Serializers k -> Tape
               -> IO (SortingEvent k)
readSortingRow (Serializers parser _) tp = do
  res <- parseFromTape (sortingEventParser parser) tp
  case res of
    Left (ctxts, err) -> fail ("readSortingRow.parseFromTape: " ++ show ctxts ++ ": " ++ show err)
    Right r -> pure r

sortingEventParser :: Parser k -> Parser (SortingEvent k)
sortingEventParser parser =
  (SortingEventRunDone  <$  Atto.word32be runMarkerWord) <|>
  (SortingEventFinished <$  Atto.word32be finishedMarkerWord)  <|>
  (SortingEventKey      <$> (Atto.word32be dataMarkerWord *> parser))

-- * MaxLength vector

data SlicedVector a
  = SlicedVector
  { orig   :: VIO.IOVector a
  , sliced :: VIO.IOVector a -- ^ Slice that ends at the last index in orig
  }

slicedFromVector :: (a -> a -> Ordering) -> V.Vector a -> IO (SlicedVector a)
slicedFromVector cmp v = do
  v' <- V.thaw v
  VIO.heapify cmp v' 0 (V.length v)
  pure $ SlicedVector v' v'

slicedLength :: SlicedVector a -> Int
slicedLength (SlicedVector _ x) = VIO.length x

slicedNull :: SlicedVector a -> Bool
slicedNull (SlicedVector _ x) = VIO.null x

slicedPushHeap :: (a -> a -> Ordering)
               -> SlicedVector a -> a
               -> IO (SlicedVector a)
slicedPushHeap cmp (SlicedVector o s) a =
  copy 0
  where
    slicedLength' = VIO.length s

    startIdx = VIO.length o - slicedLength'
    startIdx' = startIdx - 1

    sliced' = VIO.slice startIdx' (slicedLength' + 1) o

    copy curIdx
      | curIdx >= slicedLength' = do
          VIO.heapInsert cmp sliced' 0 slicedLength' a
          pure (SlicedVector o sliced')
      | otherwise = do
          x <- VIO.unsafeRead s curIdx
          VIO.unsafeWrite sliced' curIdx x
          copy (curIdx + 1)

slicedTakeWhile :: (a -> a -> Ordering)
                -> (a -> Bool)
                -> SlicedVector a
                -> IO (V.Vector a, SlicedVector a)
slicedTakeWhile cmp pred (SlicedVector o v) =
  go vLength
  where
    vLength = VIO.length v

    go 0 = fmap (, SlicedVector o (VIO.slice 0 0 o))
                (V.freeze v)
    go n = do
      keep <- pred <$> VIO.unsafeRead v 0
      if keep
        then do
          VIO.pop cmp v 0 (n - 1)
          go (n - 1)
        else do
          log ("Satisfied start " ++ show (n, vLength))
          satisfied <- if n >= vLength
                       then pure mempty
                       else V.freeze (VIO.slice n (vLength - n) v)

          let v' = VIO.slice (vLength - n) n v
          VIO.unsafeMove v' (VIO.slice 0 n v)
          pure (satisfied, SlicedVector o v')

inplaceFilterSlicedM :: (a -> a -> Ordering)
                     -> (a -> IO Bool)
                     -> SlicedVector a
                     -> IO (SlicedVector a)
inplaceFilterSlicedM cmp pred (SlicedVector o v) =
  go v 0
  where
    go v' !foundLen
      | VIO.length v' == foundLen = do
          VIO.heapify cmp v' 0 (VIO.length v')
          pure (SlicedVector o v')
      | otherwise = do
          keep <- pred =<< VIO.unsafeRead v' 0
          if keep
            then do
              VIO.swap v' 0 (VIO.length v' - foundLen - 1)
              go v' (foundLen + 1)
            else go (VIO.tail v') foundLen

data SlicedVectorPair a
  = SlicedVectorPair
  { origSliced   :: SlicedVector a
  , firstLength  :: Mut.IOPRef Int }

mkSlicedPair :: SlicedVector a -> IO (SlicedVectorPair a)
mkSlicedPair sliced' = SlicedVectorPair sliced' <$> Mut.newRef (slicedLength sliced')

firstSliceHead :: SlicedVectorPair a -> IO (Maybe a)
firstSliceHead (SlicedVectorPair (SlicedVector _ x) firstLenV) = do
  firstLen <- Mut.readRef firstLenV
  if firstLen == 0 || VIO.null x then pure Nothing
    else Just <$> VIO.unsafeRead x 0

slicedPairTakeSecond :: (a -> a -> Ordering) -> SlicedVectorPair a -> IO (SlicedVector a)
slicedPairTakeSecond cmp (SlicedVectorPair (SlicedVector orig' v) firstLenV) = do
  firstLen <- Mut.readRef firstLenV
  let v' = VIO.slice firstLen (VIO.length v - firstLen) v
  VIO.heapify cmp v' 0 (VIO.length v')
  pure (SlicedVector orig' v')

-- | Takes the first element of the first vector (which is heap structured),
-- pops it, and places it in the second vector
slicedPopHeadAndInsertSortedInFirst
  :: (a -> a -> Ordering)
  -> SlicedVectorPair a -> a
  -> IO ()
slicedPopHeadAndInsertSortedInFirst cmp (SlicedVectorPair (SlicedVector _ v) firstLenV) new = do
  firstLen <- Mut.readRef firstLenV
  VIO.pop cmp v 0 (firstLen - 1)
  VIO.heapInsert cmp v 0 (firstLen - 1) new

slicedMoveHeadToSecondAndSet :: (a -> a -> Ordering)
                             -> SlicedVectorPair a
                             -> a
                             -> IO ()
slicedMoveHeadToSecondAndSet cmp (SlicedVectorPair (SlicedVector _ v) firstLenV) a = do
  firstLen <- Mut.readRef firstLenV
  VIO.pop cmp v 0 (firstLen - 1)
  Mut.writeRef firstLenV (firstLen - 1)
  VIO.write v (firstLen - 1) a

debugFirst :: SlicedVectorPair a -> IO (V.Vector a)
debugFirst (SlicedVectorPair (SlicedVector _ v) firstLenV) = do
  firstLen <- Mut.readRef firstLenV
  V.freeze (VIO.slice 0 firstLen v)
