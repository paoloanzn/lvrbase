import asyncio
from web3 import AsyncWeb3, WebSocketProvider
from eth_abi.abi import decode

BASE_HTTP_RPC = "https://mainnet.base.org"
BASE_WSS_RPC = "wss://base.drpc.org"
POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224" # WETH/USDC 0.5% UNISWAP V3
SWAP_TOPIC_HASH = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# WETH has 18 decimals
# USDC has 6 decimals
DEC0, DEC1 = 18, 6

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
     {"input": [], "name": "liquidity",
      "outputs": [
          {"name": "type", "type": "uint128"}
      ],
      "stateMutability": "view", "type": "function"},
]

# solidity has no integers -> price is in Q format (2^k)
# on Uniswap V3 -> stored_integer = real_price * 2^96
# stored_integer / 2^96 -> raw_sqrt_price
# raw_sqrt_price^2 -> raw_price = raw_token0(WETH)/raw_token1(USDC)
# raw_token0(WETH)/raw_token1(USDC) * 10^(DEC0 - DEC1 = 12) -> USDC price
def price_from_sqrtx96(sqrtx96: int, dec0: int, dec1: int) -> float:
    raw = (sqrtx96 / (1 << 96)) ** 2
    return raw * (10 ** (dec0 - dec1))

def decode_swap_data(data_hex: str) -> dict:
    types = ["int256", "int256", "uint160", "uint128", "int24"]
    raw = bytes.fromhex(data_hex)
    amount0, amount1, sqrtx96, liq, tick = decode(types, raw)
    return {"amount0": amount0, "amount1": amount1,
            "sqrt_x96": sqrtx96, "liq": liq, "tick": tick,
            "price": price_from_sqrtx96(sqrtx96, DEC0, DEC1)}

async def main() -> None:
    async with AsyncWeb3(WebSocketProvider(BASE_WSS_RPC)) as w3:
        sub_id = await w3.eth.subscribe("logs", {
            "address": POOL,
            "topics": [SWAP_TOPIC_HASH],
        })
        print(f"subscribed: {sub_id}")
        async for payload in w3.socket.process_subscriptions():
            try:
                log = payload["result"]
                ev = decode_swap_data(log["data"].hex()
                                    if isinstance(log["data"], bytes)
                                    else log["data"])
                side = "buy_token0 " if ev["amount0"] < 0 else "sell_token0"
                print(f"block={int(str(log['blockNumber']), 16)} "
                    f"tx={log['transactionHash'].hex()[:10]} "
                    f"{side} price={ev['price']:.4f} L={ev['liq']}")
            except Exception as err:
                print(f"error: {err}")

if __name__ == "__main__":
    asyncio.run(main())