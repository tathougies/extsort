{-# LANGUAGE OverloadedStrings #-}

module Main ( main ) where

import           Data.External.Sort
import           Data.External.Tape

import           Control.Monad.Trans.Resource

import           Data.Conduit
import qualified Data.Conduit.Binary as C
import qualified Data.Conduit.List as C

import           Data.Attoparsec.Binary (anyWord64le)
import           Data.ByteString.Builder
import           Data.ByteString.Char8 (unpack)
import           Data.Monoid
import           Data.String

import           System.Environment
import           System.IO

import           Text.Read



main :: IO ()
main = do
  [input] <- getArgs

  let defaultOptions = SortOptions (10 * 1024) 7 10000 "sort.tmp" False
      intSerializers = Serializers anyWord64le word64LE
      noSerializer = Serializers (pure ()) (const mempty)

  runResourceT $ runConduit (C.sourceFile input   =$=
                             C.lines              =$=
                             C.mapMaybe (readMaybe . unpack) =$=
                             C.map (,())          =$=
                             externalSort defaultOptions
                                          compare
                                          intSerializers
                                          noSerializer
                                          noBytesCopy =$=
                             C.map ((<> "\n") . fromString . show . fst) =$=
                             C.sinkHandle stdout)
