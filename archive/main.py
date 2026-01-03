from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from helper import (
    log, clear_log,
    MAIN_MENU_ITEMS,FREQ_COLORS
)

from data_manager import (
    refresh_equity,
    refresh_indices,
    insert_equity_price_data,
    insert_index_price_data,
    refresh_52week_stats,
    refresh_indicators,
    download_daily_weekly_monthly_data
)
from scanners import (
    scanner_hilega_milega,
    backtest_scanner,
    scanner_WIP
)
from db.create_db import create_stock_database

console = Console()
################################################################################################# 
# Displays a styled console menu using Rich, listing actions with colored frequency tags 
# and prompting the user to choose an option.
#################################################################################################
def display_menu():
    table = Table.grid(padding=(0, 3))   # no spacing between rows
    table.add_column("Press")
    table.add_column("Action", style="white")
    table.add_column("Frequency", justify="center")

    for opt, action, freq, _ in MAIN_MENU_ITEMS:
        row_color = FREQ_COLORS.get(freq, "white")
        freq_text = f"[{row_color}]{freq.upper()}[/{row_color}]" if freq else ""

        # 👉 option number follows FREQUENCY color
        press_text  = f"👉 [bold {row_color}]{opt}[/bold {row_color}]"
        action_text = f"[bold]{action.upper()}[/bold]"

        # entire row styled using frequency color
        table.add_row(press_text, action_text, freq_text, style=row_color)

    panel = Panel(
        table,
        title="[bold blue]DATA MANAGER[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press [yellow]ENTER[/yellow]:[/bold green] ", end="")
################################################################################################# 
# 1.	Creates a Rich Table with headers in bold magenta.
# 2.	Adds a column to the table for each column in the DataFrame.
# 3.	Iterates over the first max_rows of the DataFrame, converting each value to a string, and adds them as rows to the table.
# 4.	Prints the table to the console.
# 5.	If the DataFrame has more rows than max_rows, prints a dim message indicating how many rows are not shown.
#################################################################################################  
def print_df_rich(df, max_rows=20):
    """Print a DataFrame in Rich Table format."""
    table = Table(show_header=True, header_style="bold magenta")

    # Add columns
    for col in df.columns:
        table.add_column(str(col))

    # Add rows (limit to max_rows)
    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(val) for val in row.values])

    console.print(table)
    if len(df) > max_rows:
        console.print(f"... [bold]{len(df) - max_rows}[/] more rows not shown", style="dim")

################################################################################################# 
# Continuously shows a menu, handles user choices to run various 
# data-management tasks (like creating the DB, refreshing symbols, and updating prices), 
# and exits when requested.
#################################################################################################
def data_manager_user_input():
    try:
        while True:
            display_menu()
            choice = Prompt.ask("[bold green]👉[/bold green]").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting...[/bold green]")
                break
            elif choice == "1":
            ##################################################################################################### 
                clear_log()
                # Create Database
                console.print("[bold green]Database Creation Start....[/bold green]")
                create_stock_database(drop_existing=True)
                console.print("[bold green]Database Creation Finish....[/bold green]")

                # Refresh Equity Symbols
                console.print("[bold green]Equity Symbols Insert Start....[/bold green]")
                refresh_equity()
                console.print("[bold green]Equity Symbols Insert Finish....[/bold green]")

                # Refresh Index Symbols
                console.print("[bold green]Index Symbols Insert Start....[/bold green]")
                refresh_indices()
                console.print("[bold green]Index Symbols Insert Finish....[/bold green]")
            ##################################################################################################### 
            elif choice == "2":
            #####################################################################################################  
                clear_log()
                # Export data from Yahoo
                console.print("[bold green]Equity Price Data Update Start....[/bold green]")
                syms = Prompt.ask("Enter symbols (either ALL or comma separated, e.g., RELIANCE,TCS)")
                insert_equity_price_data(syms.upper())
                console.print("[bold green]Equity Price Data Update Finish....[/bold green]")

                # Run Index Price Data block ONLY if user typed ALL
                if syms.strip().upper() == "ALL":
                # Index Price Data
                    console.print("[bold green]Index Price Data Update Start....[/bold green]")
                    insert_index_price_data()
                    console.print("[bold green]Index Price Data Update Finish....[/bold green]")
            #####################################################################################################  
            elif choice == "3":
            #####################################################################################################  
                console.print("[bold green]Update Equity 52 weeks statistics Start....[/bold green]")
                refresh_52week_stats()
                console.print("[bold green]Update Equity 52 weeks statistics Finish....[/bold green]")
            #####################################################################################################  
            elif choice == "4":
            #####################################################################################################  
                # Refresh Indicators
                console.print("[bold green]Refresh Indicators Start....[/bold green]")
                refresh_indicators()
                console.print("[bold green]Refresh Indicators Finish....[/bold green]")
            ##################################################################################################### 
            elif choice == "5":
            #####################################################################################################  
                clear_log()
                console.print("[bold green]ownload BhavCopy and Update Equity Price Table Start....[/bold green]")
                download_daily_weekly_monthly_data()
                console.print("[bold green]ownload BhavCopy and Update Equity Price Table Finish....[/bold green]")
            #####################################################################################################  
            elif choice == "6":
            #####################################################################################################  
                console.print("[bold green]Update Equity 52 weeks statistics Start....[/bold green]")
                refresh_52week_stats()
                console.print("[bold green]Update Equity 52 weeks statistics Finish....[/bold green]")
            #####################################################################################################  
            elif choice == "7":
            #####################################################################################################  
                console.print("[bold green]Refresh Indicators Start....[/bold green]")
                refresh_indicators()
                console.print("[bold green]Refresh Indicators Finish....[/bold green]")
            #####################################################################################################   
            elif choice == "8":
            #####################################################################################################  
                clear_log()
                # log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")  # log only when scanner runs
                df = scanner_hilega_milega()
                print_df_rich(df)
            #####################################################################################################  
            elif choice == "9":
            #####################################################################################################  
                clear_log()
                log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")  # log only when scanner runs
                df = scanner_WIP()
                print_df_rich(df)
            #####################################################################################################  
            elif choice == "10":
            ##################################################################################################### 
                clear_log()
                log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")  # log for backtest
                syms_input = Prompt.ask("[bold cyan]Enter Scanner File Name[/]")
                backtest_scanner(syms_input)
            #####################################################################################################
            else:
                console.print("[bold red]❌ Invalid choice![/bold red]")

    except KeyboardInterrupt:
        console.print("\n[bold green]Interrupted by user. Exiting...[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
################################################################################################# 
# Runs the program’s main user-interaction loop when the script 
# is executed directly (not imported).
################################################################################################# 
if __name__ == "__main__":
    data_manager_user_input()