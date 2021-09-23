{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Data.External.Tape
  ( TapeHead, Tape, TapeFile

  , tapeTapeFile, tapeReadingBlock
  , tapeRuns, tapeRecords

  , Copier(..), CopyStatus(..)
  , noBytesCopy

  , openTapeFile, closeTapeFile
  , closeReadHead, closeTapeHead

  , openTape, rewindTape
  , writeTapeBuilder, parseFromTape
  , runCopierInTape ) where

import           Prelude hiding (rem, log)

import           Control.DeepSeq (NFData, rnf)
import           Control.Exception (evaluate)
import           Control.Monad
import           Control.Monad.IO.Class

import           Data.Attoparsec.ByteString.Char8 as Atto hiding (take)
import           Data.Bits
import qualified Data.ByteString as BS (length)
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Builder.Extra as B
import qualified Data.ByteString.Internal as BS (toForeignPtr, fromForeignPtr)
import           Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
import qualified Data.PQueue.Min as PQ
import qualified Data.Mutable as Mut
import           Data.Word

import           Foreign.C.Error
import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Marshal (copyBytes, allocaBytes)
import           Foreign.Ptr
import           Foreign.Storable

import           System.Posix hiding (release)

import           GHC.Stack

newtype FileBlock = FileBlock Word64
  deriving (Show, Eq, Ord, Storable, Mut.Prim)

data TapeHeadMode = Reading | Writing
  deriving Show

data TapeHeadForeign (mode :: TapeHeadMode) where
  TapeHeadForeign :: !(ForeignPtr Word8) -> TapeHeadForeign 'Reading
  TapeHeadNoForeign :: TapeHeadForeign 'Writing

instance Show (TapeHeadForeign mode) where
  show (TapeHeadForeign ptr) = "(TapeHeadForeign " ++ show ptr ++ ")"
  show TapeHeadNoForeign = "TapeHeadNoForeign"

data TapeHead (mode :: TapeHeadMode)
    = TapeHead
    { tapeHeadData        :: !(Ptr ())
    , tapeHeadForeignData :: !(TapeHeadForeign mode)
    , tapeHeadRawSize     :: !CSize

    , tapeHeadBlockNumber :: !CSize
    , tapeHeadBlockOffset :: !CSize

    , tapeHeadFileBlock   :: !FileBlock
    } deriving Show

data Tape
    = Tape
    { tapeReadingBlock        :: IORef (Maybe (TapeHead 'Reading))
    , tapeWritingBlock        :: IORef (Maybe (TapeHead 'Writing))

    , tapeRuns                :: Mut.IOPRef Word32
    , tapeRecords             :: Mut.IOPRef Word32

    , tapeTapeFile            :: TapeFile
    }

data TapeFile
    = TapeFile
    { tapeFileHandle    :: !Fd

    , tapeFileBlockSize :: !CSize

    , tapeFileCurBlock  :: Mut.IOPRef FileBlock
    , tapeFileSize      :: Mut.IOPRef Word64

    , tapeFileFree      :: IORef (PQ.MinQueue FileBlock)
    }

newtype Copier v = Copier (Ptr v -> CSize -> IO (CopyStatus v))
data CopyStatus v
    = CopyAll (Copier v)
    | CopyOnly !CSize

-- * Constants

lastBlock :: FileBlock
lastBlock = FileBlock 0xFFFFFFFFFFFFFFFF

noBytesCopy :: Copier a
noBytesCopy = Copier $ \_ _ -> pure (CopyOnly 0)

-- * Utilities

log :: MonadIO m => String -> m ()
log _ = pure ()

plusSize :: Ptr a -> CSize -> Ptr a
plusSize p c = plusPtr p (fromIntegral c)

-- * Tape files

openTapeFile :: FilePath -> CSize -> IO TapeFile
openTapeFile tmpFilePath blockSize = do
  log ("Open tape file " ++ show tmpFilePath ++ ". block size " ++ show blockSize)
  TapeFile <$> openFd tmpFilePath ReadWrite (Just 0o700) (OpenFileFlags False False False False False)
           <*> pure blockSize
           <*> Mut.newRef (FileBlock 0)
           <*> Mut.newRef 0
           <*> newIORef mempty

closeTapeFile :: TapeFile -> IO ()
closeTapeFile file =
  closeFd (tapeFileHandle file)

-- * Tapes

openTape :: TapeFile -> IO Tape
openTape tapeFile =
  Tape <$> newIORef Nothing
       <*> newIORef Nothing

       <*> Mut.newRef 0
       <*> Mut.newRef 0

       <*> pure tapeFile

tapeUsefulBlockSize :: Tape -> CSize
tapeUsefulBlockSize tp = tapeFileBlockSize (tapeTapeFile tp) - fromIntegral (sizeOf (undefined :: FileBlock))

nextBlock :: Tape -> TapeHead 'Reading -> IO FileBlock
nextBlock tp hd = peek (castPtr (tapeHeadData hd `plusSize` tapeUsefulBlockSize tp))

setNextBlock :: Tape -> TapeHead 'Writing -> FileBlock -> IO ()
setNextBlock tp hd = poke (castPtr (tapeHeadData hd `plusSize` tapeUsefulBlockSize tp))

{-# INLINE rewindTape #-}
rewindTape :: Tape -> IO ()
rewindTape tape = do
  writeHead <- readIORef (tapeWritingBlock tape)
  writeIORef (tapeWritingBlock tape) Nothing

  rdHead <- readIORef (tapeReadingBlock tape)

  case writeHead of
    Nothing -> pure ()
    Just writeHead' -> do
      log ("Rewind at " ++ show (tapeHeadFileBlock writeHead'))

      setNextBlock tape writeHead' lastBlock

  case rdHead of
    Nothing -> writeIORef (tapeReadingBlock tape) Nothing
    Just rdHead' -> do
      rdHead'' <- headFromBlock' tape 0 (tapeHeadFileBlock rdHead')
      writeIORef (tapeReadingBlock tape) (Just rdHead'')

allocateNextBlock :: TapeFile -> IO (FileBlock, Ptr (), CSize)
allocateNextBlock tpFl = do
  free <- readIORef (tapeFileFree tpFl)

  nextAllocatedBlock@(FileBlock blkNum) <-
    case PQ.minView free of
      Nothing -> do
        filSz <- Mut.readRef (tapeFileSize tpFl)
        FileBlock curBlock <- Mut.readRef (tapeFileCurBlock tpFl)

        let nextAllocatedBlock = curBlock + 1
            newFileSz = fromIntegral filSz + tapeFileBlockSize tpFl

        Mut.writeRef (tapeFileCurBlock tpFl) (FileBlock nextAllocatedBlock)
        Mut.writeRef (tapeFileSize tpFl) (fromIntegral newFileSz)

        err <- c_ftruncate (tapeFileHandle tpFl) newFileSz
        when (err < 0) (throwErrno ("nextBlock.ftruncate: " ++ show (newFileSz, curBlock)))
        log ("New block " ++ show nextAllocatedBlock ++ ": " ++ show newFileSz ++ " " ++ show (fromIntegral nextAllocatedBlock * tapeFileBlockSize tpFl))

        pure (FileBlock curBlock)
      Just (freeBlock, free') -> do
        writeIORef (tapeFileFree tpFl) free'
        pure freeBlock

  mappedPtr <-
      c_mmap nullPtr (tapeFileBlockSize tpFl) (kPROT_READ <> kPROT_WRITE) 0x1 {- MAP_SHARED -}
             (tapeFileHandle tpFl) (fromIntegral blkNum * tapeFileBlockSize tpFl)

  when (mappedPtr == nullPtr) (throwErrno "allocateNextBlock.mmap")

  madvise mappedPtr (tapeFileBlockSize tpFl) kMADV_SEQUENTIAL
  madvise mappedPtr (tapeFileBlockSize tpFl) kMADV_WILLNEED

  log ("Allocate next block " ++ show nextAllocatedBlock)

  pure (nextAllocatedBlock, mappedPtr, tapeFileBlockSize tpFl)

headToNextBlock :: Tape -> TapeHead 'Writing -> IO (TapeHead 'Writing)
headToNextBlock tp hd = do
  (fileBlk, datPtr, rawSize) <- allocateNextBlock (tapeTapeFile tp)
  let hd' = TapeHead datPtr TapeHeadNoForeign rawSize 0 0 fileBlk

  setNextBlock tp hd fileBlk

  pure hd'

-- * Tape heads

tapeHeadCurData :: TapeHead mode -> Ptr ()
tapeHeadCurData hd =
  tapeHeadData hd `plusSize` tapeHeadBlockOffset hd

tapeHeadSizeLeft :: Tape -> TapeHead mode -> CSize
tapeHeadSizeLeft tp hd =
  tapeUsefulBlockSize tp - tapeHeadBlockOffset hd

advanceHead :: TapeHead mode -> CSize -> TapeHead mode
advanceHead hd sz =
    hd { tapeHeadBlockOffset = tapeHeadBlockOffset hd + sz }

closeReadHead :: TapeFile -> TapeHead 'Reading -> IO ()
closeReadHead fl hd = do
  closeTapeHead hd

  modifyIORef' (tapeFileFree fl) (PQ.insert (tapeHeadFileBlock hd))

closeTapeHead :: TapeHead mode -> IO ()
closeTapeHead hd = do
  err <- c_munmap (tapeHeadData hd) (tapeHeadRawSize hd)
  when (err < 0) (throwErrno "closeTapeHead.munmap")

-- ** Output with tape heads

{-# INLINE writeTapeGeneric #-}
writeTapeGeneric :: Tape -> (TapeHead 'Writing -> IO (a, TapeHead 'Writing)) -> IO a
writeTapeGeneric tp go = do
  hd <- readIORef (tapeWritingBlock tp)

  hd' <- case hd of
           Nothing -> do
             (fileBlk, datPtr, rawSize) <- allocateNextBlock (tapeTapeFile tp)
             foreignPtr <- newForeignPtr_ (castPtr datPtr)
             let hd' = TapeHead datPtr TapeHeadNoForeign rawSize 0 0 fileBlk
                 readHd = TapeHead datPtr (TapeHeadForeign foreignPtr) rawSize 0 0 fileBlk

             rdHd <- readIORef (tapeReadingBlock tp)
             case rdHd of
               Nothing -> writeIORef (tapeReadingBlock tp) (Just readHd)
               Just {} -> writeIORef (tapeReadingBlock tp) rdHd

             pure hd'
           Just hd' -> pure hd'

  (x, !hd'') <- go hd'

  writeIORef (tapeWritingBlock tp) (Just hd'')
  pure x

{-# INLINE writeTapeBuilder #-}
writeTapeBuilder :: Tape -> B.Builder -> IO ()
writeTapeBuilder tp builder =
  writeTapeGeneric tp (go (B.runBuilder builder))
  where
    maxMoreSize = 4096

    go writer !hd =
      let szLeft = tapeHeadSizeLeft tp hd
      in if szLeft == 0
         then do
           hd' <- headToNextBlock tp hd
           closeTapeHead hd

           go writer hd'
         else do
           (bytesWritten, next) <- writer (castPtr (tapeHeadCurData hd)) (fromIntegral szLeft)
           let hd' = advanceHead hd (fromIntegral bytesWritten)

           case next of
             B.Done -> pure ((), hd')
             B.More sz next'
               | sz < maxMoreSize -> do
                   cont <- allocaBytes maxMoreSize $ \ptr ->
                           fillRequest ptr sz next' hd'
                   cont
               | otherwise -> fail "writeTapeBuilder: B.More: out of memory"
             B.Chunk bs next' -> do
               let (bsPtr, bsOff, bsSz) = BS.toForeignPtr bs
               hd'' <- withForeignPtr bsPtr $ \ptr ->
                       writeTapeHeadBuf tp hd' (ptr `plusPtr` bsOff) (fromIntegral bsSz)
               go next' hd''

    fillRequest buf reqSz next !hd
      | reqSz > maxMoreSize = fail "writeTapeBuilder.fillRequest: out of memory"
      | otherwise = do
          (written, res') <- next buf reqSz

          hd' <- writeTapeHeadBuf tp hd buf (fromIntegral written)
          case res' of
            B.Done -> pure (pure ((), hd'))
            B.More sz next' -> fillRequest buf sz next' hd'
            B.Chunk bs next' -> do
              let (bsPtr, bsOff, bsSz) = BS.toForeignPtr bs
              hd'' <- withForeignPtr bsPtr $ \ptr ->
                        writeTapeHeadBuf tp hd' (ptr `plusPtr` bsOff) (fromIntegral bsSz)
              pure (go next' hd'')

writeTapeHeadBuf :: Tape -> TapeHead 'Writing -> Ptr a -> CSize -> IO (TapeHead 'Writing)
writeTapeHeadBuf tp hd cpyPtr cpySz =
  let remainingBytes = tapeHeadSizeLeft tp hd
  in if cpySz <= remainingBytes
     then do
       copyBytes (tapeHeadCurData hd) (castPtr cpyPtr) (fromIntegral cpySz)
       pure (advanceHead hd cpySz)
     else do
       copyBytes (tapeHeadCurData hd) (castPtr cpyPtr) (fromIntegral remainingBytes)

       hd' <- headToNextBlock tp hd
       closeTapeHead hd

       writeTapeHeadBuf tp hd' (cpyPtr `plusSize` remainingBytes) (cpySz - remainingBytes)

-- ** Input with tape heads

headFromBlock :: Tape -> TapeHead 'Reading -> IO (TapeHead 'Reading)
headFromBlock tp hd = do
  nextBlockNumber <- nextBlock tp hd
  when (nextBlockNumber == lastBlock) $
    fail "headFromBlock: end of tape"

  log ("Getting next head " ++ show nextBlockNumber)
  headFromBlock' tp (tapeHeadBlockNumber hd + 1) nextBlockNumber

headFromBlock' :: Tape -> CSize -> FileBlock -> IO (TapeHead 'Reading)
headFromBlock' tp seqNum (FileBlock nextBlockNumber) =
  do let hdl = tapeFileHandle fl
         fl = tapeTapeFile tp

     mappedPtr <- c_mmap nullPtr (tapeFileBlockSize fl) (kPROT_READ <> kPROT_WRITE) 0x1 {- MAP_SHARED -}
                         hdl (fromIntegral nextBlockNumber * tapeFileBlockSize fl)
     when (mappedPtr == nullPtr) (throwErrno "headFromBlock.mmap")

     madvise mappedPtr (tapeFileBlockSize fl) kMADV_SEQUENTIAL
     madvise mappedPtr (tapeFileBlockSize fl) kMADV_WILLNEED

     log ("Head from block mapped " ++ show mappedPtr)

     foreignPtr <- newForeignPtr_ (castPtr mappedPtr)
     pure (TapeHead mappedPtr (TapeHeadForeign foreignPtr) (tapeFileBlockSize fl) seqNum 0 (FileBlock nextBlockNumber))

withReadHead :: Tape -> (TapeHead 'Reading-> IO (a, TapeHead 'Reading)) -> IO a
withReadHead tp go =
  do rdHead <- readIORef (tapeReadingBlock tp)
     case rdHead of
       Nothing -> fail "withReadHead: No reading block!"
       Just rdHead' ->
         do (x, rdHead'') <- go rdHead'
            writeIORef (tapeReadingBlock tp) (Just rdHead'')
            pure x

{-# INLINE parseFromTape #-}
parseFromTape :: (HasCallStack, NFData k)
              => Atto.Parser k -> Tape
              -> IO (Either ([String], String) k)
parseFromTape parser tp =
  do log "parseFromTape"
     withReadHead tp (parseFromTapeHead (Atto.Partial (parse parser)))

  where
    blockSz = tapeUsefulBlockSize tp

    parseFromTapeHead (Done _ r) !hd = do
      log "Tape head done"
      evaluate (rnf r)
      pure (Right r, hd)
    parseFromTapeHead (Fail _ ctxts err) !hd =
      log "Tape head fail" >>
      pure (Left (ctxts, err), hd)
    parseFromTapeHead (Partial next) !hd =
      let remainingBytes = tapeHeadSizeLeft tp hd
      in if remainingBytes == 0
         then do
           log "parseFromTape crosses boundary"
           hd' <- headFromBlock tp hd
           log "Got new head"

           r <- parseFromTapeHead (Partial next) hd'
           closeReadHead (tapeTapeFile tp) hd

           pure r
         else do
           log "parseFromTape block"

           let chunk = BS.fromForeignPtr foreignData
                                         (fromIntegral $ tapeHeadBlockOffset hd)
                                         (fromIntegral remainingBytes)
               TapeHeadForeign foreignData = tapeHeadForeignData hd

               nextRes = next chunk
               hd' = case nextRes of
                       Partial {} -> hd { tapeHeadBlockOffset = blockSz }
                       Done rem _ -> advanceHead hd (remainingBytes - fromIntegral (BS.length rem))
                       Fail rem _ _ -> advanceHead hd (remainingBytes - fromIntegral (BS.length rem))

           parseFromTapeHead nextRes hd'

{-# INLINE runCopierInTape #-}
runCopierInTape :: HasCallStack => Copier v -> Tape -> Tape -> IO ()
runCopierInTape copier inputTp outputTp =
  writeTapeGeneric outputTp $ \outputHd ->
  withReadHead outputTp $ \readHd ->
  doCopy copier readHd outputHd

  where
    doCopy (Copier runCopy) readHd outputHd
      | tapeHeadSizeLeft inputTp readHd == 0 = do
          readHd' <- headFromBlock inputTp readHd
          closeReadHead (tapeTapeFile inputTp) readHd

          doCopy (Copier runCopy) readHd' outputHd
      | otherwise = do
          let sizeLeft = tapeHeadSizeLeft inputTp readHd
              curData = tapeHeadCurData readHd

          status <- runCopy (castPtr curData) sizeLeft

          case status of
            CopyAll next -> do
              outputHd' <- writeTapeHeadBuf outputTp outputHd curData sizeLeft
              doCopy next (advanceHead readHd sizeLeft) outputHd'
            CopyOnly 0  -> do
              pure (((), outputHd), readHd)
            CopyOnly sz -> do
              outputHd' <- writeTapeHeadBuf outputTp outputHd curData sz
              pure (((), outputHd'), advanceHead readHd sz)

-- * Foreign calls

newtype MmapProtection = MmapProtection CInt deriving Show
instance Monoid MmapProtection where
    mempty = MmapProtection 0

instance Semigroup MmapProtection where
    MmapProtection a <> MmapProtection b = MmapProtection (a .|. b)

kPROT_EXEC, kPROT_READ, kPROT_WRITE :: MmapProtection
kPROT_READ = MmapProtection 0x1
kPROT_WRITE = MmapProtection 0x2
kPROT_EXEC = MmapProtection 0x4

newtype MAdvice = MAdvice CInt deriving Show

kMADV_SEQUENTIAL, kMADV_WILLNEED :: MAdvice
kMADV_SEQUENTIAL = MAdvice 2
kMADV_WILLNEED = MAdvice 3

madvise :: Ptr a -> CSize -> MAdvice -> IO ()
madvise ptr sz adv =
  throwErrnoIf_ (==(-1)) "madvise() failed:"
    (c_madvise (castPtr ptr) sz adv)

foreign import ccall "unistd.h ftruncate" c_ftruncate :: Fd -> CSize -> IO CInt
foreign import ccall "sys/mman.h mmap" c_mmap :: Ptr () -> CSize -> MmapProtection -> CInt -> Fd -> CSize -> IO (Ptr ())
foreign import ccall "sys/mman.h munmap" c_munmap :: Ptr () -> CSize -> IO CInt
foreign import ccall "sys/mman.h madvise" c_madvise :: Ptr () -> CSize -> MAdvice -> IO CInt
