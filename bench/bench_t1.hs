import Control.Concurrent
import Chan.KickChan
import Control.Monad
import Control.Monad.Fix
import Data.Vector.Mutable
import Control.Concurrent.Async

-- benchmark throughput, two writers...
main = do
  kc <- newKickChan 10
    :: IO (KickChan IOVector Int)

  -- spawn writer
  c1 <- async $ mkWriter kc
  c2 <- async $ mkWriter kc
  waitBoth c1 c2

maxSize = 1000000

mkWriter kc = forM_ [0::Int .. maxSize] $ \i -> do
    putKickChan kc i
