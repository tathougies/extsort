module Data.External.Sort where

import           Prelude hiding (log, read)

import           Data.External.Tape hiding (log)

import           Control.Applicative ((<|>))
import           Control.DeepSeq (NFData)
import           Control.Monad (when, unless, forM_, forM)
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
import qualified Data.Mutable as Mut (readRef, writeRef, modifyRef')
import           Data.Ord (comparing)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as VIO (sortByBounds)
import qualified Data.Vector.Mutable as VIO
import           Data.Word (Word8, Word32)

import           Foreign.C.Types

import           GHC.Generics

import           System.IO
import           System.Posix

data SortingEvent k
    = SortingEventRunDone
    | SortingEventKey k
    | SortingEventFinished
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

removeAt :: Int -> V.Vector a -> V.Vector a
removeAt ix = do
  (before, after) <- V.splitAt ix
  if V.null after then pure before else pure (before <> V.tail after)

log, info:: MonadIO m => String -> m ()
-- log = liftIO . putStrLn
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

          when (curSz == maxSz) $ do
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
            . ( Show k, NFData k )
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
       nwayMerges' (V.zip inputs tapes) initialOutputTape

  where
    nwayMerges' :: V.Vector (SortingEvent k, Tape)
                -> Tape -> IO Tape
    nwayMerges' inputs outputTape = do
      (nextOutputTape, inputs') <- nwayMerge' inputs outputTape

      rewindTape outputTape

      inputs'' <- V.filterM (fmap (> 0) . Mut.readRef . tapeRuns . snd) inputs'

      if V.null inputs''
         then info "No more inputs" >> pure outputTape
         else do
           info "Sort again"
           nextOutputSize <- Mut.readRef (tapeRuns nextOutputTape)
           when (nextOutputSize > 0) (fail "nextOutputSize > 0")

           outputHd <- readIORef (tapeReadingBlock nextOutputTape)
           writeIORef (tapeReadingBlock nextOutputTape) Nothing

           case outputHd of
             Nothing -> pure ()
             Just outputHd' -> closeTapeHead outputHd'

           outRow <- readSortingRow keySerializer outputTape
           info ("Out row is " ++ show outRow)
           info ("Inputs are " ++ show (fmap fst inputs''))

           nwayMerges' (V.cons (outRow, outputTape) inputs'') nextOutputTape

    nwayMerge' :: V.Vector (SortingEvent k, Tape) -> Tape
               -> IO (Tape, V.Vector (SortingEvent k, Tape))
    nwayMerge' inputs outputTape
      | V.null inputs = do
          log "Inputs null"
          writeTapeBuilder outputTape (runMarker <> finishedMarker)
          pure (outputTape, inputs)

      | otherwise = do
          inputs' <- mergeRun inputs mempty outputTape
          Mut.modifyRef' (tapeRuns outputTape) (+1)
          writeTapeBuilder outputTape runMarker

          let (finished, alive) =
                  V.partition (isSortingFinished . fst) inputs'
          case finished V.!? 0 of
            Nothing -> do
              log "None finish"
              nwayMerge' inputs' outputTape
            Just (_, nextOutput) -> do
              -- Release all the non-output tapes that are done now
              forM_ finished $ \(_, finishedTp) -> do
                rdHead <- readIORef (tapeReadingBlock finishedTp)
                writeIORef (tapeReadingBlock finishedTp) Nothing

                case rdHead of
                  Nothing -> pure ()
                  Just rdHead' -> closeReadHead (tapeTapeFile finishedTp) rdHead'

              -- We're now one with this tape
              writeTapeBuilder outputTape finishedMarker

              pure (nextOutput, alive)

    mergeRun :: V.Vector (SortingEvent k, Tape)
             -> V.Vector (SortingEvent k, Tape)
             -> Tape -> IO (V.Vector (SortingEvent k, Tape))
    mergeRun inputs runDone outputTape = do
      (inputs', runDone') <-
        V.foldM (\(inputs', runDone') (i, (x, tp)) ->
                     case x of
                       SortingEventRunDone -> do
                         log ("Row done " ++ show i)
                         x' <- readSortingRow keySerializer tp
                         log "Row done!"
                         let runDone'' = V.cons (x', tp) runDone'
                             inputs'' = removeAt i inputs'
                         Mut.modifyRef' (tapeRuns tp) (subtract 1)
                         pure (inputs'', runDone'')
                       SortingEventFinished -> fail "File finished before run done"
                       _ -> pure (inputs', runDone'))
                (inputs, runDone)
                (V.indexed inputs)

      if V.null inputs'
        then log "mergeRun done" >> pure runDone'
        else do
          inputs'' <- doCompare inputs' outputTape
          mergeRun inputs'' runDone' outputTape

    doCompare :: V.Vector (SortingEvent k, Tape) -> Tape
              -> IO (V.Vector (SortingEvent k, Tape))
    doCompare inputs outputTape = do
      log "Compare"
      let keyedInput = V.imap (\i (SortingEventKey key, tp) -> (i, (key, tp))) inputs

          (inputTpIdx, (lowestKey, inputTp)) = V.minimumBy (cmpKey `on` (fst . snd)) keyedInput

      writeTapeBuilder outputTape (dataMarker <> serialBuilder keySerializer lowestKey)
      runCopierInTape valueCopier inputTp outputTape
      log "Wrote out"

      x' <- readSortingRow keySerializer inputTp
      log "Read row"
      let inputs' = inputs V.// [(inputTpIdx, (x', inputTp))]
      pure inputs'

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
