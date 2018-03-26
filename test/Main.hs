{-# LANGUAGE OverloadedStrings #-}

module Main ( main ) where

import           Data.External.Sort
import           Data.External.Tape

import           Control.Applicative

import qualified Data.Attoparsec.ByteString.Char8 as Atto
import           Data.Attoparsec.ByteString.Char8 (decimal, satisfy)
import qualified Data.Attoparsec.ByteString.Streaming as AS
import           Data.ByteString.Builder (word64Dec) -- word64Host)
import qualified Data.ByteString.Builder.Prim as P
import qualified Data.ByteString.Streaming.Char8 as BSS
import qualified Data.ByteString.Internal as BS
import           Data.Char (isSpace)
import           Data.Word

import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable

import           System.Environment
import           System.IO
import           System.IO.Unsafe

import qualified Streaming.Prelude as S

word64HostP :: Atto.Parser Word64
word64HostP = do
  x <- Atto.take 8
  let ( bsPtr, bsOff, bsSz ) = BS.toForeignPtr x
  if bsSz < 8 then fail "word64Host" else
    pure . unsafePerformIO . withForeignPtr bsPtr $ \ptr ->
      peek (ptr `plusPtr` bsOff)

main :: IO ()
main = do
  [input] <- getArgs

  let defaultOptions = SortOptions (10 * 1024) 7 100000 "sort.tmp" False
      intSerializers = Serializers word64HostP (P.primFixed P.word64Host) --LE
      noSerializer   = Serializers (pure ()) (const mempty)

      word64Parser = decimal <* many (satisfy (\c -> c /= '\n' && isSpace c)) <* satisfy (=='\n')

  withFile input ReadMode $ \input' ->
    BSS.toHandle stdout $
    BSS.unlines $
    S.subst (\(w, ()) -> BSS.toStreamingByteString (word64Dec w)) $
    externalSort defaultOptions compare intSerializers noSerializer noBytesCopy $
    S.map (,()) $
    AS.parsed word64Parser $
    BSS.fromHandle input'
