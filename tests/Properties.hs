{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Main (main) where

import Chan.KickChan

import Control.Applicative
import Control.Concurrent
import Control.Monad
import Control.Monad.Fix
import Data.Vector.Mutable (IOVector)

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
  , testProperty "currentLag" checkLag
  , testCase "blocking read" checkBlockRead
  , testCase "invalid read"  checkInvalidating
  , testCase "full buffer read" checkTail
  , testCase "invalidating channels" checkKill
  , testCase "simple run" raceTest
  ]

-- check writes are non-blocking
checkNBWrites :: IO ()
checkNBWrites = do
    c <- kcUnboxed <$> newKickChan 2
    H.assertEqual "correct size" 2 (kcSize c)
    mapM_ (\i -> putKickChan c i) [1..5::Int]

-- check that a new reader is initialized to the head of a KickChan
checkReadHead :: NonEmptyList Int -> Property
checkReadHead (NonEmpty (x:xs)) = monadicIO $ do
    c <- run $ kcDefault <$> newKickChan 5
    run $ mapM_ (putKickChan c) xs
    r <- run $ newReader c
    run $ putKickChan c x
    Just x' <- run $ readNext r
    assert $ x == x'

-- check write/read pairs stay synced
checkReads :: [Int] -> Property
checkReads xs = monadicIO $ do
    c <- run $ kcStorable <$> newKickChan 2
    r <- run $ newReader c
    let checkEl x = do
          run $ putKickChan c x
          Just x' <- run $ readNext r
          assert (x==x')
    mapM_ checkEl xs

-- check that blocking reads work
checkBlockRead :: IO ()
checkBlockRead = do
    c <- kcUnboxed <$> newKickChan 2
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
    c <- kcUnboxed <$> newKickChan 3
    H.assertEqual "correct size" 4 (kcSize c)
    r <- newReader c
    mapM_ (putKickChan c) [1..5::Int]
    b <- readNext r
    H.assertEqual "reader should be invalid" Nothing b

checkTail :: IO ()
checkTail = do
    c <- kcUnboxed <$> newKickChan 3
    H.assertEqual "correct size" 4 (kcSize c)
    r <- newReader c
    let xs = [1..4::Int]
    mapM_ (putKickChan c) xs
    xs' <- replicateM 4 $ readNext r
    H.assertEqual "full buffer read" (map Just xs) xs'

checkLag :: NonNegative Int -> NonNegative Int -> Property
checkLag (NonNegative readLn) (NonNegative writeDiff) = (readLn < 2048 && writeDiff < 2048) ==> (monadicIO $ do
    c <- run $ kcUnboxed <$> newKickChan 8
    r <- run $ newReader c
    lag <- run $ do
        replicateM_ (readLn + writeDiff) (putKickChan c (1::Int))
        replicateM_ readLn (readNext r)
        currentLag r
    assert $ lag == writeDiff )

checkKill :: IO ()
checkKill = do
    c :: KickChanU Int <- kcUnboxed <$> newKickChan 4
    r <- newReader c
    invalidateKickChan c
    b <- readNext r
    H.assertEqual "reader should be invalid" Nothing b
    r <- newReader c
    resultvar <- newEmptyMVar
    forkIO $ readNext r >>= putMVar resultvar
    invalidateKickChan c
    x' <- takeMVar resultvar
    H.assertEqual "waiters should be invalid" Nothing x'

-- check that we get the expected results with 2 writers and 2 readers.
-- This doesn't prove it's correct, but it can show if it's wrong.
raceTest = do
  kc <- newKickChan 10
    :: IO (KickChan IOVector (Either Int Int))
  rdr1 <- newReader kc
  rdr2 <- newReader kc

  -- spawn writer
  _ <- forkIO $ mkWriter kc Left
  _ <- forkIO $ mkWriter kc Right

  forkIO $ mkReader rdr1 (either (const Nothing) Just)
  mkReader rdr2 (either Just (const Nothing))

numIters = 40000

mkWriter kc proj = forM_ [0::Int .. numIters] $ \i -> do
    putKickChan kc (proj i)
    when (mod i 2 == 0) $ threadDelay 100

mkReader rdr dir = flip fix 0 $ \self expected -> if expected > numIters then return () else do
    v <- (fmap . fmap) dir $ readNext rdr
    case v of
        Nothing -> error "reader got Nothing..."
        Just Nothing -> self expected
        Just (Just x) | x == expected -> self $ expected + 1
                      | otherwise -> error $ "expected " ++ show expected ++ " but got " ++ show x
