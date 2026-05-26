import asyncio, aiohttp, time
from web3 import AsyncWeb3, WebSocketProvider
from web3.exceptions import ProviderConnectionError, TaskNotRunning, TimeExhausted, Web3RPCError
from eth_abi.abi import decode
from websockets.exceptions import WebSocketException
import math
from collections import deque

BASE_HTTP_RPC = "https://mainnet.base.org"
BASE_WSS_RPC = "wss://base.drpc.org"
POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224" # WETH/USDC 0.5% UNISWAP V3
SWAP_TOPIC_HASH = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
RECONNECT_DELAY_SECONDS = 2
MAX_RECONNECT_DELAY_SECONDS = 60
BACKOFF_RESET_SECONDS = 60
# WETH has 18 decimals
# USDC has 6 decimals
DEC0, DEC1 = 18, 6
PYTH_HERMES_BASE = "https://hermes.pyth.network/v2/updates/price/latest"
PYTH_ETH_USD_ID = ("0xff61491a931112ddf1bd8147"
                   "cd1b641375f79f5825126d6654"
                   "80874634fd0ace")
PYTH_HERMES = f"{PYTH_HERMES_BASE}?ids[]={PYTH_ETH_USD_ID}"
WINDOW_SEC = 300 # 5 MIN rolling window
SEC_PER_YEAR = 3600 * 24 * 365

# in Uniswap V3 the running price is stored in store0()
# the active liquidity L is exposed by liquidity()
POOL_ABI = [
    {"inputs": [], "name": "slot0",
     "outputs": [
         {"name": "sqrtPriceX96", "type": "uint160"},
         {"name": "tick", "type": "int24"},
         {"name": "obsIndex", "type": "uint16"},
         {"name": "obsCard", "type": "uint16"},
         {"name": "obsCardNext", "type": "uint16"},
         {"name": "feeProtocol", "type": "uint8"},
         {"name": "unlocked", "type": "bool"}],
         "stateMutability": "view", "type": "function"},
     {"inputs": [], "name": "liquidity",
      "outputs": [
          {"name": "type", "type": "uint128"}
      ],
      "stateMutability": "view", "type": "function"},
]

# solidity has no floats -> price is stored as an int -> Q format (2^k)
# on Uniswap V3 -> stored_integer = real_price * 2^96
# stored_integer / 2^96 -> raw_sqrt_price
# raw_sqrt_price^2 -> raw_price = raw_token0(WETH)/raw_token1(USDC)
# raw_token0(WETH)/raw_token1(USDC) * 10^(DEC0 - DEC1 = 12) -> USDC price
def price_from_sqrtx96(sqrtx96: int, dec0: int, dec1: int) -> float:
    raw = (sqrtx96 / (1 << 96)) ** 2
    return raw * (10 ** (dec0 - dec1))

class RealizedVol:
    def __init__(self, window_sec: int = WINDOW_SEC) -> None:
        self.window_sec = window_sec
        self.samples: deque = deque()
    
    def update(self, t: float, px: float) -> None:
        if px <= 0 or not math.isfinite(px):
            return
        self.samples.append((t, math.log(px)))
        # drop out-of-window samples
        cutoff = t - self.window_sec
        # pop every sample with t older than cutoff
        # asc order -> pop the leftmost until nothing is left
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()
    
    def volatility_annual(self) -> float:
        if len(self.samples) < 3 or (self.samples[-1][0] - self.samples[0][0]) < WINDOW_SEC * 0.98:
            return float("nan")
        rv = 0.0
        _, prev_lp = self.samples[0]
        for t, lp in list(self.samples)[1:]:
            rv += (lp - prev_lp) ** 2
            _, prev_lp = t, lp

        # rv = variance -> variance = volatility ^ 2
        # scale WINDOW_SEC volatility to a year duration
        return math.sqrt(rv * SEC_PER_YEAR / self.window_sec)

async def pyth_poller(out: asyncio.Queue) -> None:
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(PYTH_HERMES, timeout=2.0) as r:
                    j = await r.json()
                feed = j["parsed"][0]["price"]
                px = int(feed["price"]) * (10 ** int(feed["expo"]))
                await out.put((time.time(), float(px)))
            except Exception as err:
                print(f"pyth-error: {err if err else 'NaN'}")
            await asyncio.sleep(1.0)

# last reference price polled from pyth
class Latest:
    def __init__(self) -> None:
        self.t = 0.0
        self.px = float("nan")
    
    def update(self, t: float, px: float) -> None:
        self.t, self.px = t, px

async def consume_pyth(q: asyncio.Queue, ref: Latest, rv: RealizedVol) -> None:
    while True:
        t, px = await q.get()
        ref.update(t, px)
        rv.update(t, px)

# swap event from Uniswap V3
# event Swap(
# address indexed sender, 
# address indexed recipient,
# int256 amount0,
# int256 amount1,
# uint160 sqrtPriceX96,
# uint128 liquidity,
# int24 tick)
# 
# --> this signature get's hashed into SWAP_TOPIC_HASH
def decode_swap_data(data_hex: str) -> dict:
    types = ["int256", "int256", "uint160", "uint128", "int24"]
    raw = bytes.fromhex(data_hex.removeprefix("0x"))
    amount0, amount1, sqrtx96, liq, tick = decode(types, raw)
    return {"amount0": amount0, "amount1": amount1,
            "sqrt_x96": sqrtx96, "liq": liq, "tick": tick,
            "price": price_from_sqrtx96(sqrtx96, DEC0, DEC1)}

def lvr_rate_dollars_per_sec(volatility_ann: float, liquidity: int, price: float, dec0: int, dec1: int) -> float:
    if not (math.isfinite(volatility_ann) and price > 0 and liquidity > 0):
        return float("nan")
    # convert raw liq into human L
    l_h = liquidity / (10 ** ((dec0 + dec1) / 2))
    variance_per_sec = (volatility_ann ** 2) / SEC_PER_YEAR
    return variance_per_sec * l_h * math.sqrt(price) / 4.0

def lvr_rate_dollars_per_year(volatility_ann: float) -> float:
    return (volatility_ann ** 2) / 8.0 * 10_000 # bps/year

# total liquidity pool value in token1 -> liquidity pool value in USDC
v = lambda p, liq: 2 * (liq / (10 ** ((DEC0 + DEC1) / 2))) * math.sqrt(p)

def hex_value(value) -> str:
    return value.hex() if hasattr(value, "hex") else str(value)

def int_value(value) -> int:
    return value if isinstance(value, int) else int(str(value), 16)

class WindowedAccumulator:
    def __init__(self, window_sec: int = 3600) -> None:
        self.window_sec = window_sec
        self.events: deque = deque()
        self.lvr_cum = 0.0
        self.fee_cum = 0.0

    def add(self, t: float, lvr_inc: float, fee_inc: float) -> None:
        self.events.append((t, lvr_inc, fee_inc))
        self.lvr_cum += lvr_inc
        self.fee_cum += fee_inc
        cutoff = t - self.window_sec
        while self.events and self.events[0][0] < cutoff:
            _, lvi, fei = self.events.popleft()
            self.lvr_cum -= lvi
            self.fee_cum -= fei

    def ratio(self) -> float:
        return self.fee_cum / self.lvr_cum if self.lvr_cum > 0 else float("inf")

# compute fee revenue on one swap given fee tier
def fee_dollars_from_swap(amount0: int, amount1: int, price_usdc_per_weth: float, 
                          fee_pips: int=500,
                          dec0: int=DEC0, dec1: int=DEC1) -> float:
    notional_usdc = (abs(amount0) / 10 ** dec0) * price_usdc_per_weth + (abs(amount1) / 10**dec1)
    notional_usdc /= 2
    return notional_usdc * (fee_pips / 1_000_000)

# decode Swap() event from logs
async def stream_swaps(ref: Latest, rv: RealizedVol) -> None:
    async with AsyncWeb3(WebSocketProvider(BASE_WSS_RPC)) as w3:
        # logs shape
        # address   <-- contract_that_emitted_it
        # topics
        # data      <--- raw ABI-encoded bytes
        sub_id = await w3.eth.subscribe("logs", {
            "address": POOL,
            "topics": [SWAP_TOPIC_HASH],
        })
        print(f"subscribed: {sub_id}")
        async for payload in w3.socket.process_subscriptions():
            try:
                log = payload["result"]
                ev = decode_swap_data(hex_value(log["data"]))
                # amount0 > 0  means the pool received WETH
                # amount0 < 0  means the pool sent WETH out
                side = "buy_token0 " if ev["amount0"] < 0 else "sell_token0"
                block_number = int_value(log["blockNumber"])
                
                gap_bps = ((ev["price"] - ref.px) / ref.px) * 10_000 if ref.px == ref.px else float("nan")
                volatility_ann = rv.volatility_annual()

                lvr_per_sec = lvr_rate_dollars_per_sec(volatility_ann, ev['liq'], ev['price'], DEC0, DEC1)
                lvr_bps_yr = lvr_rate_dollars_per_year(volatility_ann)
                print(f"block={block_number} "
                      f"tx={hex_value(log['transactionHash'])[:10]} "
                      f"{side} price={ev['price']:.4f} L={ev['liq']} "
                      f"pyth={ref.px:.4f} gap={gap_bps:+.2f}bps "
                      f"volatility_ann={volatility_ann*100:.1f}% "
                      f"lvr_pool_$/hr={lvr_per_sec:.2f} "
                      f"lvr_bps_yr={lvr_bps_yr:.2f}")
            except Exception as err:
                print(f"error: {err}")


RETRYABLE_STREAM_ERRORS = (
    WebSocketException,
    ProviderConnectionError,
    Web3RPCError,
    TaskNotRunning,
    TimeExhausted,
    asyncio.IncompleteReadError,
    asyncio.TimeoutError,
    OSError,
)


async def stream_swaps_with_retries(ref: Latest, rv: RealizedVol) -> None:
    delay = RECONNECT_DELAY_SECONDS

    while True:
        started = time.monotonic()
        try:
            await stream_swaps(ref, rv)
        except RETRYABLE_STREAM_ERRORS as err:
            if time.monotonic() - started >= BACKOFF_RESET_SECONDS:
                delay = RECONNECT_DELAY_SECONDS
            print(
                f"swap-stream-error: {type(err).__name__}: {err}; "
                f"reconnecting in {delay:.1f}s"
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)
        else:
            print(f"swap-stream-ended; reconnecting in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)



async def main() -> None:
    q = asyncio.Queue(maxsize=8)
    ref = Latest()
    rv = RealizedVol()
    pyth_tasks = [
        asyncio.create_task(pyth_poller(q)),
        asyncio.create_task(consume_pyth(q, ref, rv)),
    ]
    try:
        await stream_swaps_with_retries(ref, rv)
    finally:
        for task in pyth_tasks:
            task.cancel()
        await asyncio.gather(*pyth_tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
