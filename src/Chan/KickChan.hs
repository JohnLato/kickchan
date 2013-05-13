{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wall #-}
module Chan.KickChan (
  -- * Kick Channels
  KickChan
-- ** Creation
, newKickChan
, kcSize
-- ** Writing
, putKickChan
-- ** Reading
, KCReader
, newReader
, readNext
) where

import Control.Applicative
import Control.Concurrent.MVar

import Data.Bits
import Data.Data
import Data.IORef

import Data.Vector.Generic.Mutable (MVector)
import qualified Data.Vector.Generic.Mutable as M

import qualified Data.Vector.Unboxed.Mutable as U
import Control.Monad.Primitive

#if MIN_VERSION_base(4,6,0)
-- nothing to do here...
#else
atomicModifyIORef' :: IORef a -> (a -> (a,b)) -> IO b
atomicModifyIORef' ref f = do
    b <- atomicModifyIORef ref
            (\x -> let (a, b) = f x
                    in (a, a `seq` b))
    b `seq` return b
#endif

-- internal structure to hold the channel head
data Position a = Position {-# UNPACK #-} !Int [MVar a]

{- invariants
 - the Int is the next position to be written (buffer head, initialized to 0)
 - the Int changes monotonically
 - the [MVar a] are actions to be run when the next value is ready
-}

emptyPosition :: Position a
emptyPosition = Position 0 []

-- increment a Position, returning the old position
incrPosition :: Position a -> (Position a,Position a)
incrPosition orig@(Position oldP _) = (newP,orig)
  where
    newP = Position (oldP+1) []

data CheckResult = Await | Ok | Invalid

checkWithPosition :: MVar a -> Int -> Int -> Position a -> (Position a, CheckResult)
checkWithPosition await sz readP pos@(Position nextP acts) = case nextP - readP of
    0 -> (Position nextP (await:acts), Await) -- add a waiter to the current position
    dif | dif > 0 && dif <= sz -> (pos,Ok)
        | otherwise -> (pos,Invalid) -- requests that are too old or too far in the future

-- | A Channel that drops elements from the end when a 'KCReader' lags too far
-- behind the writer.
data KickChan a = KickChan
    { kcSz  :: {-# UNPACK #-} !Int
    , kcPos :: (IORef (Position a))
    , kcV   :: (U.MVector RealWorld a)
    } deriving (Typeable)

{- invariants
 - kcSz is a power of 2
 -}

-- | Create a new 'KickChan' of the requested size.  The actual size will be
-- rounded up to the next highest power of 2.
newKickChan :: (MVector U.MVector a) => Int -> IO (KickChan a)
newKickChan sz = do
    kcPos <- newIORef emptyPosition
    kcV <- M.new kcSz
    return KickChan {..}
  where
    kcSz = 2^(ceiling (logBase 2 (fromIntegral $ sz) :: Double) :: Int)
{-# INLINABLE newKickChan #-}

-- | Get the size of a 'KickChan'.
kcSize :: KickChan a -> Int
kcSize KickChan {kcSz} = kcSz

-- | Put a value into a 'KickChan'.
--
-- O(k), where k == number of readers on this channel.
--
-- putKickChan will never block, instead 'KCReader's will be invalidated if
-- they lag too far behind.
putKickChan :: (MVector U.MVector a) => KickChan a -> a -> IO ()
putKickChan  KickChan {..} x = do
    (Position curSeq pending) <- atomicModifyIORef' kcPos incrPosition
    M.unsafeWrite kcV (curSeq .&. (kcSz-1)) x
    mapM_ (\v -> putMVar v x) pending
{-# INLINE putKickChan #-}

-- | get a value from a 'KickChan', or 'Nothing' if no longer available.
-- 
-- O(1)
getKickChan :: (MVector U.MVector a) => KickChan a -> Int -> IO (Maybe a)
getKickChan KickChan {..} readP = do
    x <- M.unsafeRead kcV (readP .&. (kcSz-1))
    await <- newEmptyMVar
    result <- atomicModifyIORef' kcPos (checkWithPosition await kcSz readP)
    case result of
        Ok    -> return $ Just x
        Await -> Just <$> takeMVar await
        Invalid -> return Nothing

-- | A reader for a 'KickChan'
data KCReader a = KCReader
    { kcrChan :: {-# UNPACK #-} !(KickChan a)
    , kcrPos  :: IORef Int
    }

{- invariants
 - kcrPos is the position of the most recently read element (initialized to -1)
 -}

-- | create a new reader for a 'KickChan'.  The reader will be initialized to
-- the head of the KickChan, so that an immediate call to 'readNext' will
-- block (provided no new values have been put into the chan in the meantime).
newReader :: KickChan a -> IO (KCReader a)
newReader kcrChan@KickChan{..} = do
    (Position writeP _) <- readIORef kcPos   
    kcrPos <- newIORef (writeP-1)
    return KCReader {..}
{-# INLINABLE newReader #-}

-- | get the next value from a 'KCReader'.  This function will block if the next
-- value is not yet available.
--
-- if Nothing is returned, the reader has lagged the writer and values have
-- been dropped.
readNext :: (MVector U.MVector a) => KCReader a -> IO (Maybe a)
readNext (KCReader {..}) = do
    readP <- atomicModifyIORef' kcrPos (\lastP -> let p = lastP+1 in (p,p))
    getKickChan kcrChan readP
{-# INLINE readNext #-}
