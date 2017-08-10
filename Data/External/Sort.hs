{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Data.External.Sort
  ( Serializers(..), SortOptions(..)

  , externalSort ) where

#include "MachDeps.h"

import           Prelude hiding (log, read, pred, mapM)

import           Data.External.Tape

import           Control.Arrow
import           Control.DeepSeq (NFData)
import           Control.Monad (when, unless, forM_, forM, join)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT) --, allocate)

#ifdef WORDS_BIGENDIAN
import qualified Data.Attoparsec.Binary as Atto (anyWord32be)
#else
import qualified Data.Attoparsec.Binary as Atto (anyWord32le)
#endif

import           Data.Attoparsec.ByteString.Char8 as Atto hiding (take)
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Builder.Extra as B
--import           Data.Conduit ((=$=), Conduit, ConduitM, await, yield)
import           Data.Foldable (msum)
import           Data.Function (on, (&))
import           Data.IORef (readIORef, writeIORef)
import           Data.List (genericReplicate, tails, minimumBy)
import           Data.Monoid ((<>))
import qualified Data.Mutable as Mut (IOPRef, newRef, readRef, writeRef, modifyRef')
import           Data.Ord (comparing)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Heap as VIO (heapify, pop, heapInsert)
import qualified Data.Vector.Algorithms.Intro as VIO (sortByBounds)
import qualified Data.Vector.Mutable as VIO
import           Data.Word (Word8, Word32)

import           Foreign.C.Types

import           GHC.Stack

import           Streaming

import           System.IO
import           System.Posix

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

runSzBuilder :: Word32 -> B.Builder
runSzBuilder = B.word32Host

-- * Utilities

flipOrd :: Ordering -> Ordering
flipOrd EQ = EQ
flipOrd LT = GT
flipOrd GT = LT

debugMVector :: VIO.IOVector a -> (V.Vector a -> IO ()) -> IO ()
debugMVector _ _ = pure ()
--debugMVector v g = g =<< V.freeze v

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

{-# SPECIALIZE externalSort :: (NFData k, NFData v, Show k)
                            => SortOptions -> (k -> k -> Ordering) -> Serializers k -> Serializers v
                            -> Copier v -> Stream (Of (k, v)) (ResourceT IO) a
                            -> Stream (Of (k, v)) (ResourceT IO) () #-}
externalSort :: ( MonadResource m, MonadIO m, NFData k, NFData v, Show k )
             => SortOptions
             -> (k -> k -> Ordering)
             -> Serializers k
             -> Serializers v
             -> Copier v
             -> Stream (Of (k, v)) m a
             -> Stream (Of (k, v)) m ()
externalSort options cmpKey keySerializers valueSerializers valueCopier input =
  do let unalignedBlockSize = (sortOptionsWorkingMem options * 1024) `div`
                              fromIntegral (sortOptionsTapes options)
         blockSize = ((unalignedBlockSize + kBLOCK_ALIGNMENT - 1) `div` kBLOCK_ALIGNMENT) *
                     kBLOCK_ALIGNMENT

     when (blockSize <= kMIN_BLOCK_SIZE) (fail "externalSort: Not enough space")

     bracketStream (openTapeFile (sortOptionsWorkingFile options) blockSize)
                   closeTapeFile $ \tapeFile -> do
       unless (sortOptionsPersistFile options) $
         liftIO (removeLink (sortOptionsWorkingFile options))

       info "[SORT] Reading input"
       tapes <- lift (sinkInitialTapes options keySerializers valueSerializers tapeFile .
                       sortChunks cmpKey (sortOptionsChunkSize options) $ input)

       info "[SORT] [MERGE] Starting..."
       outputTape <-
           liftIO $ do
             forM_ (V.tail tapes) rewindTape
             nwayMerges (V.tail tapes) (V.head tapes) cmpKey keySerializers valueCopier
       info "[SORT] [MERGE] Done..."

       sourceAllRawRows keySerializers valueSerializers outputTape

       info "[SORT] Done"

{-# SPECIALIZE sortChunks :: (k -> k -> Ordering) -> Int
                          -> Stream (Of (k, v)) (ResourceT IO) a
                          -> Stream (Compose (Of Int) (Stream (Of (k, v)) (ResourceT IO))) (ResourceT IO) a #-}
sortChunks :: MonadIO m => (k -> k -> Ordering) -> Int
           -> Stream (Of (k, v)) m a
           -> Stream (Compose (Of Int) (Stream (Of (k, v)) m)) m a
sortChunks cmpKey chunkSz inputs = do
  v <- liftIO (VIO.new chunkSz)

  inputs & (chunksOf chunkSz >>>
            mapsM (\s -> streamFold (\res tuplesRead -> do
                                        liftIO (info "Read chunk")
                                        liftIO (VIO.sortByBounds (cmpKey `on` fst) v 0 tuplesRead)
                                        pure (Compose (tuplesRead :>
                                                       (unfold (\curIdx ->
                                                                  if curIdx >= tuplesRead
                                                                  then pure (Left res)
                                                                  else Right . (:> (curIdx + 1)) <$>
                                                                       liftIO (VIO.unsafeRead v curIdx)) 0 ))))
                                    (\action i -> do
                                        next <- action
                                        next i)
                                    (\(cur :> next) i -> do
                                        liftIO (VIO.unsafeWrite v i cur)
                                        next (i +1)) s 0))

{-# SPECIALIZE sourceAllRawRows :: (NFData k, NFData v) => Serializers k -> Serializers v
                                -> Tape -> Stream (Of (k, v)) (ResourceT IO) () #-}
-- | Read all rows
sourceAllRawRows :: (MonadIO m, NFData v, NFData k)
                 => Serializers k -> Serializers v
                 -> Tape
                 -> Stream (Of (k, v)) m ()
sourceAllRawRows keySerializers valueSerializers tp =
  concats .
  flip unfold () $ \() -> do
  runsLeft <- liftIO (Mut.readRef (tapeRuns tp))
  if runsLeft == 0
    then pure (Left ())
    else do
      liftIO $ Mut.writeRef (tapeRuns tp) (runsLeft - 1)

      res <- liftIO $ parseFromTape runSzParser tp
      case res of
        Left (ctxt, err) -> fail ("sourceAllRows: no parse: " ++ show ctxt ++ ": " ++ err)
        Right tupleCount ->
          pure . Right $
          flip unfold tupleCount $ \n ->
            if n == 0 then pure (Left ())
            else do
              kv <- liftIO (parseFromTape ((,) <$> serialParser keySerializers <*> serialParser valueSerializers) tp)
              case kv of
                Left (ctxt, err) -> fail ("sourceAllRows: no parse kv: " ++ show ctxt ++ ": " ++ err)
                Right kv' -> pure (Right (kv' :> n - 1))

{-# SPECIALIZE sinkInitialTapes :: SortOptions -> Serializers k -> Serializers v
                                -> TapeFile -> Stream (Compose (Of Int) (Stream (Of (k, v)) (ResourceT IO))) (ResourceT IO) x
                                -> ResourceT IO (V.Vector Tape) #-}
-- | Distribute initial runs
sinkInitialTapes
    :: ( MonadResource m, MonadIO m )
    => SortOptions
    -> Serializers k
    -> Serializers v
    -> TapeFile
    -> Stream (Compose (Of Int) (Stream (Of (k, v)) m)) m x
    -> m (V.Vector Tape)
sinkInitialTapes SortOptions { sortOptionsTapes = tapeCount }
                 keySerializers valueSerializers
                 tapeFile inputs =
  do tapes <- liftIO (V.replicateM (fromIntegral tapeCount) (openTape tapeFile))

     let sizes = kthFibonacci (fromIntegral (tapeCount - 1))

     (totalSz, outputTapeSizes) <- readRuns inputs tapes 0 sizes

     -- Add dummy runs
     liftIO . forM_ (V.zip (V.fromList outputTapeSizes) (V.tail tapes)) $ \(tapeSz, tape) -> do
         recs <- Mut.readRef (tapeRuns tape)

         let dummies = fromIntegral tapeSz - recs
             dummyBuilder = mconcat (genericReplicate dummies (runSzBuilder 0))

         writeTapeBuilder tape dummyBuilder
         Mut.writeRef (tapeRuns tape) tapeSz

     info ("Tape sizes " ++ show outputTapeSizes ++ " = " ++ show totalSz)

     pure tapes

  where
    readRuns =
      streamFold (\_ _ _ sizes ->
                    case sizes of
                      [] -> error "impossible"
                      (maxSz,sizes'):_ -> pure (maxSz, sizes'))
                 (\action tapes curSz sizes -> do
                     next <- action
                     next tapes curSz sizes)
                 (\(Compose (runSz :> runStream)) tapes curSz oldSizes ->
                    case oldSizes of
                      [] -> error "impossible"
                      ((maxSz, sizes):sizess) -> do
                        let (nextSizes, (maxSz', sizes')) =
                              if curSz == maxSz then (sizess, head sizess) else (oldSizes, (maxSz, sizes))

                        (_, outputTape) <-
                          liftIO (chooseNextTape tapes sizes')

                        liftIO (writeTapeBuilder outputTape (runSzBuilder (fromIntegral runSz)))

                        next <- iterT (\((key, value) :> next') -> do
                                          liftIO (writeTapeBuilder outputTape (serialBuilder keySerializers key <> serialBuilder valueSerializers value))
                                          next') runStream

                        when (curSz == maxSz) $
                          info ("Going to use " ++ show maxSz')

                        next tapes (curSz + 1) nextSizes)

--    readRuns _ _ [] = error "impossible"
--    readRuns tapes curSz oldSizes@((maxSz, sizes):sizess) = do
--      nextTuples <- await
--      case nextTuples of
--        Nothing -> pure (maxSz, sizes)
--        Just tuples -> do
--          let (nextSizes, (maxSz', sizes')) =
--                  if curSz == maxSz then (sizess, head sizess) else (oldSizes, (maxSz, sizes))
--
--          (_, outputTape) <-
--              liftIO (chooseNextTape tapes sizes')
--
--          let builder = runSzBuilder (fromIntegral (V.length tuples)) <>
--                        foldMap (\(key, value) -> serialBuilder keySerializers key <> serialBuilder valueSerializers value) tuples
--          liftIO (writeTapeBuilder outputTape builder)
--
--          when (curSz == maxSz) $
--            info ("Going to use " ++ show maxSz')
--
--          readRuns tapes (curSz + 1) nextSizes

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
       inputs <-
         fmap msum . forM tapes $ \tp -> do
           runsLeft <- Mut.readRef (tapeRuns tp)

           if runsLeft == 0
             then pure mempty
             else do
               runSz <- readRunSize tp
               Mut.writeRef (tapeRecords tp) runSz
               if runSz == 0
                  then pure (pure (Nothing, tp))
                  else pure . (, tp) . Just <$> readKey tp

       join (nwayMerges' <$> slicedFromVector cmpKey' inputs
                         <*> pure initialOutputTape)

  where
    cmpKey' (a, _) (b, _) =  flipOrd (cmpMaybe cmpKey a b)

    cmpMaybe :: (a -> a -> Ordering)
             -> Maybe a -> Maybe a
             -> Ordering
    cmpMaybe _ Nothing Nothing = EQ
    cmpMaybe _ Nothing _ = LT
    cmpMaybe _ _ Nothing = GT
    cmpMaybe cmpKey'' (Just a) (Just b) = cmpKey'' a b

    readKey :: Tape -> IO k
    readKey tp = do
      r <- parseFromTape (serialParser keySerializer) tp
      case r of
        Left (ctxt, err) -> fail ("readKey: no parse " ++ show ctxt ++ ": " ++ err)
        Right r' -> pure r'

    nwayMerges' :: SlicedVector (Maybe k, Tape)
                -> Tape -> IO Tape
    nwayMerges' inputs outputTape = do
      (nextOutputTape, inputs') <- nwayMerge' inputs outputTape
      rewindTape outputTape

      debugMVector (sliced inputs') $ \inputs'Frozen ->
        info ("Frozen inputs " ++ show (fmap fst inputs'Frozen))

      inputs'' <- inplaceFilterSlicedM cmpKey'
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

           runsLeft <- Mut.readRef (tapeRuns outputTape)

           inputs'''' <-
             if runsLeft == 0
             then pure inputs''
             else do
               outputSz <- readRunSize outputTape
               Mut.writeRef (tapeRecords outputTape) outputSz

               outRow <- if outputSz == 0
                         then pure Nothing
                         else Just <$> readKey outputTape
               log ("Out row is " ++ show outRow)
               inputs''' <- slicedPushHeap cmpKey' inputs'' (outRow, outputTape)
               log "Push heap"
               pure inputs'''

           nwayMerges' inputs'''' nextOutputTape

    nwayMerge' :: SlicedVector (Maybe k, Tape) -> Tape
               -> IO (Tape, SlicedVector (Maybe k, Tape))
    nwayMerge' inputs outputTape
      | slicedNull inputs = do
          info "Inputs null"
          pure (outputTape, inputs)

      | otherwise = do
          debugMVector (sliced inputs) $ \inputsFrozen ->
            log ("Inputs " ++ show (fmap fst inputsFrozen))

          allRecords <- sumSliced inputs $ \(_, tp) -> Mut.readRef (tapeRecords tp)
          writeTapeBuilder outputTape (runSzBuilder allRecords)

          inputs' <- join (mergeRun <$> mkSlicedPair inputs
                                    <*> pure outputTape)
          log "Run merged"
          debugMVector (sliced inputs') $ \inputs'Frozen ->
            log ("Inputs' " ++ show (fmap fst inputs'Frozen))

          Mut.modifyRef' (tapeRuns outputTape) (+1)

          log "take while start"
          (lastExcluded, alive) <-
            slicedFilter cmpKey' inputs' $ \(_, tp) -> do
              runsLeft <- Mut.readRef (tapeRuns tp)

              -- Release all the non-output tapes that are done now
              if runsLeft == 0
                then do
                  log "Finish tape head"
                  rdHead <- readIORef (tapeReadingBlock tp)
                  writeIORef (tapeReadingBlock tp) Nothing

                  case rdHead of
                    Nothing -> pure ()
                    Just rdHead' -> closeReadHead (tapeTapeFile tp) rdHead'

                  pure Nothing
                else do
                  nextRunSz <- readRunSize tp
                  Mut.writeRef (tapeRecords tp) nextRunSz

                  if nextRunSz == 0
                    then pure (Just (Nothing, tp))
                    else do
                      nextKey <- readKey tp
                      pure (Just (Just nextKey, tp))
          log "take while end"

          case lastExcluded of
            Nothing -> do
              info "None finish"
              debugMVector (sliced inputs') $ \inputs'Frozen' ->
                log ("Inputs' frozen after none " ++ show (fmap fst inputs'Frozen'))
              nwayMerge' inputs' outputTape
            Just (_, nextOutput) -> do
              -- We're now done with this tape
              log "wrote finished"

              debugMVector (sliced alive) $ \aliveFrozen ->
                log ("Alive before loop " ++ show (fmap fst aliveFrozen))

              pure (nextOutput, alive)

    mergeRun :: HasCallStack
             => SlicedVectorPair (Maybe k, Tape)
             -> Tape -> IO (SlicedVector (Maybe k, Tape))
    mergeRun inputsAndDone outputTape = do
      firstInput <- firstSliceHead inputsAndDone
      case firstInput of
        Nothing -> do
          log "mergeRun done"
          slicedPairTakeSecond cmpKey' inputsAndDone
        Just (Nothing, tp) -> do
          slicedMoveHeadToSecondAndSet cmpKey' inputsAndDone (Nothing, tp)
          Mut.modifyRef' (tapeRuns tp) (subtract 1)

          mergeRun inputsAndDone outputTape
        Just (Just lowestKey, tp) -> do
          recordsLeft <- subtract 1 <$> Mut.readRef (tapeRecords tp)

          writeTapeBuilder outputTape (serialBuilder keySerializer lowestKey)
          runCopierInTape valueCopier tp outputTape

          Mut.writeRef (tapeRecords tp) recordsLeft

          x' <- if recordsLeft == 0
                then pure Nothing
                else Just <$> readKey tp
          slicedPopHeadAndInsertSortedInFirst cmpKey' inputsAndDone (x', tp)

          mergeRun inputsAndDone outputTape

readRunSize :: Tape -> IO Word32
readRunSize tp =
  do res <- parseFromTape runSzParser tp
     case res of
       Left (ctxts, err) ->
         fail ("readRunSize : parse error: " ++ show ctxts ++ ": " ++ err)
       Right res' ->
         pure res'

runSzParser :: Atto.Parser Word32
#ifdef WORDS_BIGENDIAN
runSzParser = Atto.anyWord32be
#else
runSzParser = Atto.anyWord32le
#endif

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
  go
  where
    slicedLength' = VIO.length s

    startIdx = VIO.length o - slicedLength'
    startIdx' = startIdx - 1

    sliced' = VIO.slice startIdx' (slicedLength' + 1) o

    go = do
      VIO.unsafeMove (VIO.slice 0 slicedLength' sliced') s
      VIO.heapInsert cmp sliced' 0 slicedLength' a
      pure (SlicedVector o sliced')

slicedFilter :: (a -> a -> Ordering)
             -> SlicedVector a
             -> (a -> IO (Maybe a))
             -> IO (Maybe a, SlicedVector a)
slicedFilter cmp (SlicedVector o v) pred =
  go 0 vLength Nothing
  where
    vLength = VIO.length v

    go i e lastExcluded
      | i < e = do
          x  <- VIO.unsafeRead v i
          x' <- pred x
          case x' of
            Nothing -> go (i + 1) e (Just x)
            Just e' -> do
              VIO.unsafeSwap v i (e - 1)
              VIO.unsafeWrite v (e - 1) e'
              go i (e - 1) lastExcluded
      | otherwise = do
          let v' = VIO.unsafeSlice i (vLength - i) v
          VIO.heapify cmp v' 0 (vLength - i)
          pure (lastExcluded, SlicedVector o v')

sumSliced :: Num o => SlicedVector a -> (a -> IO o) -> IO o
sumSliced (SlicedVector _ v) f =
  go 0 0
  where
    vLength = VIO.length v

    go n !a
      | n < vLength = do
          x <- VIO.unsafeRead v n
          s <- f x
          go (n + 1) (a + s)
      | otherwise = pure a

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
              VIO.unsafeSwap v' 0 (VIO.length v' - foundLen - 1)
              go v' (foundLen + 1)
            else go (VIO.unsafeTail v') foundLen

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
  let v' = VIO.unsafeSlice firstLen (VIO.length v - firstLen) v
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
  VIO.unsafeWrite v (firstLen - 1) a
