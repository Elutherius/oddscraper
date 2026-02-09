"""
CLI entrypoint for Polymarket Universe Snapshot tool.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .runner import print_summary, run_fetch
from .utils import utc_date_str
from .filters import filter_markets_by_category


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="pm_universe",
        description="Polymarket Universe Daily Pricing Snapshot Tool",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch markets and prices")
    fetch_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date for snapshot (YYYY-MM-DD), defaults to today UTC",
    )
    fetch_parser.add_argument(
        "--outdir",
        type=str,
        default="data",
        help="Output directory (default: data)",
    )
    fetch_parser.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive mode to select category",
    )
    fetch_parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Limit number of markets (for testing)",
    )
    fetch_parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="CLOB batch concurrency (default: 5)",
    )
    fetch_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="CLOB batch size (default: 500)",
    )
    fetch_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch Gamma only, skip pricing",
    )
    fetch_parser.add_argument(
        "--gamma-rate",
        type=float,
        default=2.0,
        help="Gamma API requests per second (default: 2.0)",
    )
    fetch_parser.add_argument(
        "--gamma-page-size",
        type=int,
        default=500,
        help="Gamma API page size (default: 500, max ~500)",
    )
    fetch_parser.add_argument(
        "--clob-rate",
        type=float,
        default=1.0,
        help="CLOB API requests per second (default: 1.0)",
    )
    fetch_parser.add_argument(
        "--active",
        action="store_true",
        help="Fetch only active markets",
    )
    fetch_parser.add_argument(
        "--sports-only",
        action="store_true",
        help="Fetch only major sports leagues (NBA, NFL, NHL, MLB, NCAAB, CFB) using tag_ids",
    )
    fetch_parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Filter by category (case-insensitive substring) during fetch",
    )
    fetch_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    # filter command
    filter_parser = subparsers.add_parser("filter", help="Filter markets CSV by category")
    filter_parser.add_argument(
        "--category",
        type=str,
        required=True,
        help="Category to filter by (case-insensitive substring match)",
    )
    filter_parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input CSV path (defaults to latest markets CSV)",
    )
    filter_parser.add_argument(
        "--output",
        type=str,
        default="filtered_markets.csv",
        help="Output CSV path",
    )
    filter_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "fetch":
        setup_logging(args.verbose)

        tag_id = None
        
        try:
            if args.interactive:
                print("Fetching available categories...")
                # We need a temporary Gamma client to fetch tags
                from .gamma import GammaClient
                from .utils import RateLimiter
                
                # Quick fetch tags
                try:
                    # Initialize rate limiter with 2 requests per second
                    with GammaClient(RateLimiter(2)) as gamma:
                        tags = gamma.fetch_tags()
                        
                    # Filter for useful tags (e.g. valid slug)
                    valid_tags = [t for t in tags if t.get("slug")]
                    # Sort by label or slug
                    valid_tags.sort(key=lambda x: x.get("label") or x.get("slug"))
                    
                    print("\nAvailable Categories:")
                    print("0. All Markets (Universe Snapshot)")
                    
                    display_tags = valid_tags
                        
                    for i, tag in enumerate(display_tags, 1):
                        label = tag.get("label") or tag.get("slug")
                        print(f"{i}. {label}")
                        
                    choice = input("\nSelect a category (0 for All): ").strip()
                    
                    if choice == "0":
                        print("Selected: All Markets")
                        tag_id = None
                    else:
                        try:
                            idx = int(choice) - 1
                            if 0 <= idx < len(display_tags):
                                selected_tag = display_tags[idx]
                                tag_id = str(selected_tag.get("id"))
                                print(f"Selected: {selected_tag.get('label') or selected_tag.get('slug')} (ID: {tag_id})")
                            else:
                                print("Invalid selection, defaulting to All Markets.")
                        except ValueError:
                            print("Invalid input, defaulting to All Markets.")
                            
                except Exception as e:
                    print(f"Error fetching tags for interactive mode: {e}")
                    print("Proceeding with All Markets.")

            if args.category and not args.interactive:
                # Try to resolve category name to tag_id for server-side filtering
                try:
                    from .gamma import GammaClient
                    from .utils import RateLimiter
                    
                    print(f"Resolving category '{args.category}' to tag ID...")
                    # Initialize rate limiter with 2 requests per second
                    with GammaClient(RateLimiter(2)) as gamma:
                        tags = gamma.fetch_tags()
                        
                    # Find matching tag
                    match = None
                    search_term = args.category.lower()
                    
                    for tag in tags:
                        label = (tag.get("label") or "").lower()
                        slug = (tag.get("slug") or "").lower()
                        
                        if search_term == label or search_term == slug:
                            match = tag
                            break
                    
                    if match:
                        tag_id = str(match.get("id"))
                        print(f"Resolved '{args.category}' to Tag ID: {tag_id} ({match.get('label')})")
                    else:
                        print(f"No exact tag match found for '{args.category}'. Using client-side filtering only.")
                        
                except Exception as e:
                    print(f"Warning: Could not resolve tag ID: {e}")

            # Define major sports series_ids if --sports-only is enabled
            # series_id returns actual game markets, while tag_id returns event groups
            sports_series_ids = None
            if args.sports_only:
                sports_series_ids = [
                    10345,    # NBA
                    10187,    # NFL
                    10346,    # NHL
                    3,        # MLB
                    10470,    # NCAAB (College Basketball)
                    10210,    # CFB (College Football)
                ]
                print(f"Sports-only mode: fetching {len(sports_series_ids)} major sports leagues")
            
            manifest = run_fetch(
                date_str=args.date,
                outdir=Path(args.outdir) if args.outdir else None,
                tag_id=tag_id,
                max_markets=args.max_markets,
                concurrency=args.concurrency,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                gamma_rate=args.gamma_rate,
                gamma_page_size=args.gamma_page_size,
                clob_rate=args.clob_rate,
                active_only=args.active,
                category_filter=args.category,
                sports_series_ids=sports_series_ids,
            )
            print_summary(manifest)
            return 0
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            return 130
        except Exception as e:
            logging.error(f"Fatal error: {e}", exc_info=True)
            return 1

    if args.command == "filter":
        setup_logging(args.verbose)
        
        try:
            # Determine input path
            if args.input:
                input_path = Path(args.input)
            else:
                # Try to find today's file or latest
                date_str = utc_date_str()
                default_path = Path("data") / "markets" / f"markets_{date_str}.csv"
                if default_path.exists():
                    input_path = default_path
                else:
                    # Fallback to just "markets.csv" or similar if needed, 
                    # but for now let's just error if explicit path not found
                    logging.error(f"Input file not specified and {default_path} not found.")
                    return 1

            output_path = Path(args.output)
            count = filter_markets_by_category(input_path, output_path, args.category)
            print(f"Filtered {count} rows matching '{args.category}' to {output_path}")
            return 0
        except Exception as e:
            logging.error(f"Filter error: {e}", exc_info=True)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
