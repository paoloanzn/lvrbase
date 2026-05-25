import asyncio
from web3 import AsyncWeb3, AsyncHTTPProvider

BASE_HTTP_RC = "https://mainnet.base.org"

async def main() -> None:
    w3 = AsyncWeb3(AsyncHTTPProvider(BASE_HTTP_RC))
    chain_id = await w3.eth.chain_id
    block = await w3.eth.get_block("latest")
    print(f"chain_id={chain_id} "
          f"block={block.number} "
          f"timestamp={block.timestamp} "
          f"gas_used={block.gasUsed}")
    
if __name__ == "__main__":
    asyncio.run(main())