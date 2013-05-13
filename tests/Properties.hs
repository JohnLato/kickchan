{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Chan.KickChan

import Control.Concurrent
import Control.Monad

import qualified Test.HUnit as H
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Test.Framework (Test, defaultMain)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.Framework.Providers.HUnit (testCase)

main :: IO ()
main = defaultMain tests

tests :: [Test]
tests =
  [ testCase "nonblocking write" checkNBWrites
  , testProperty "reader initial head" checkReadHead
  , testProperty "write/read sync" checkReads
  , testCase "blocking read" checkBlockRead
  , testCase "invalid read"  checkInvalidating
  , testCase "full buffer read" checkTail
  ]

-- check writes are non-blocking
checkNBWrites :: IO ()
checkNBWrites = do
    c <- newKickChan 2
    H.assertEqual "correct size" 2 (kcSize c)
    mapM_ (\i -> putKickChan c i) [1..5::Int]

-- check that a new reader is initialized to the head of a KickChan
checkReadHead :: NonEmptyList Int -> Property
checkReadHead (NonEmpty (x:xs)) = monadicIO $ do
    c <- run $ newKickChan 5
    run $ mapM_ (putKickChan c) xs
    r <- run $ newReader c
    run $ putKickChan c x
    Just x' <- run $ readNext r
    assert $ x == x'

-- check write/read pairs stay synced
checkReads :: [Int] -> Property
checkReads xs = monadicIO $ do
    c <- run $ newKickChan 2
    r <- run $ newReader c
    let checkEl x = do
          run $ putKickChan c x
          Just x' <- run $ readNext r
          assert (x==x')
    mapM_ checkEl xs

-- check that blocking reads work
checkBlockRead :: IO ()
checkBlockRead = do
    c <- newKickChan 2
    H.assertEqual "correct size" 2 (kcSize c)
    r <- newReader c
    resultvar <- newEmptyMVar
    forkIO $ do
        Just v <- readNext r
        putMVar resultvar v
    threadDelay 1000
    putKickChan c (23::Int)
    x' <- takeMVar resultvar
    H.assertEqual "blocking read" 23 x'

checkInvalidating :: IO ()
checkInvalidating = do
    c <- newKickChan 3
    H.assertEqual "correct size" 4 (kcSize c)
    r <- newReader c
    mapM_ (putKickChan c) [1..5::Int]
    b <- readNext r
    H.assertEqual "reader should be invalid" Nothing b

checkTail :: IO ()
checkTail = do
    c <- newKickChan 3
    H.assertEqual "correct size" 4 (kcSize c)
    r <- newReader c
    let xs = [1..4::Int]
    mapM_ (putKickChan c) xs
    xs' <- replicateM 4 $ readNext r
    H.assertEqual "full buffer read" (map Just xs) xs'
