module Main where

import Data.Word

import System.Environment
import System.Random
import System.IO

main :: IO ()
main = do
  x <- randomRIO (minBound, maxBound :: Word64)
  putStrLn (show x)
  main
