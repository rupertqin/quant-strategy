"""Data Refresh Script - Refresh data for crontab"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from DataHub.services.data_service import DataService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def refresh_prices(service: DataService) -> bool:
    """Refresh price data"""
    logger.info("Starting price refresh...")
    result = service.refresh_prices()

    if result["status"] == "success":
        logger.info(f"Price refresh successful: {result['records']} rows, {result['symbols']} symbols")
        return True
    else:
        logger.error(f"Price refresh failed: {result.get('message', 'Unknown error')}")
        return False


def refresh_zt_pool(service: DataService, date: str = None) -> bool:
    """Refresh ZT pool data"""
    date = date or service.provider.get_latest_trading_date()
    logger.info(f"Starting ZT pool refresh for {date}...")
    result = service.refresh_zt_pool(date)

    if result["status"] == "success":
        logger.info(f"ZT pool refresh successful: {result['records']} records for {result['date']}")
        return True
    else:
        logger.error(f"ZT pool refresh failed: {result.get('message', 'Unknown error')}")
        return False


def cleanup(service: DataService, days: int = 90) -> bool:
    """Clean up old data"""
    logger.info(f"Cleaning up data older than {days} days...")
    result = service.cleanup_old_data(days)
    logger.info(f"Cleaned up {result.get('zt_pool_deleted', 0)} ZT pool files")
    return True


def status(service: DataService) -> None:
    """Print data status"""
    status = service.get_data_status()

    print("\n" + "=" * 50)
    print("DataHub Status")
    print("=" * 50)
    print(f"Stock count: {status.get('stock_count', 'N/A')}")
    print(f"\nPrices:")
    print(f"  Exists: {status['prices'].get('exists', False)}")
    if status['prices'].get('exists'):
        print(f"  Size: {status['prices'].get('size_bytes', 0) / 1024:.1f} KB")
        print(f"  Modified: {status['prices'].get('modified', 'N/A')}")

    print(f"\nReturns:")
    print(f"  Exists: {status['returns'].get('exists', False)}")
    if status['returns'].get('exists'):
        print(f"  Size: {status['returns'].get('size_bytes', 0) / 1024:.1f} KB")

    print(f"\nZT Pool:")
    print(f"  Available dates: {status.get('zt_pool_dates', 0)}")

    print("\nVersions:")
    for dtype, version in status.get('versions', {}).items():
        print(f"  {dtype}: v{version['version']} @ {version['updated_at']}")

    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(description="DataHub Data Refresh Script")
    parser.add_argument("task", choices=["prices", "zt_pool", "all", "cleanup", "status"],
                       help="Task to perform")
    parser.add_argument("--date", type=str, help="Date for ZT pool (YYYYMMDD)")
    parser.add_argument("--days", type=int, default=90, help="Days to keep for cleanup")

    args = parser.parse_args()

    service = DataService()

    success = True

    if args.task == "prices":
        success = refresh_prices(service)

    elif args.task == "zt_pool":
        success = refresh_zt_pool(service, args.date)

    elif args.task == "all":
        success = refresh_prices(service)
        if success:
            success = refresh_zt_pool(service, args.date)

    elif args.task == "cleanup":
        cleanup(service, args.days)

    elif args.task == "status":
        status(service)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
