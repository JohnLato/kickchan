import Control.Concurrent
import Control.Concurrent.Async
import Chan.KickChan
import Control.Monad
import Control.Monad.Fix
import Data.Vector.Mutable
import Data.Chronograph

-- benchmark throughput...
main = do
  kc <- newKickChan 60
    :: IO (KickChan IOVector (Either Int Int))
  rdr1 <- newReader kc
  rdr2 <- newReader kc

  c1 <- async $ mkReader rdr1 (either (const Nothing) Just)
  c2 <- async $ mkReader rdr2 (either Just (const Nothing))

  -- spawn writer
  _ <- forkIO $ mkWriter kc Left
  _ <- forkIO $ mkWriter kc Right

  _ <- waitBoth c1 c2
  return ()

maxSize = 100000

mkWriter kc proj = forM_ [0::Int .. maxSize] $ \i -> do
    chronoTraceEventIO "putKickChan" <=< chronoIO $ putKickChan kc (proj i)
    when (rem i 2 == 0) $ threadDelay 100

mkReader rdr dir = flip fix 0 $ \self expected -> if expected > maxSize then return () else do
    v <- (fmap . fmap) dir $ readNext rdr
    case v of
        Nothing -> error "reader got Nothing..."
        Just Nothing -> self expected
        Just (Just x) | x == expected -> self $ expected + 1
                      | otherwise -> error $ "expected " ++ show expected ++ " but got " ++ show x
