# backtest_service.py

import os
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
from db.connection import get_db_connection, close_db_connection
from config.paths import SCANNER_FOLDER

def run_scanner_dashboard(file_name: str):
    """Run a professional Dash app to visualize scanner CSV with OHLC & volume."""
    # -----------------------------
    # Load scanner CSV
    # -----------------------------
    scanner_path = os.path.join(SCANNER_FOLDER, file_name)
    if not os.path.exists(scanner_path):
        print(f"❌ File not found: {scanner_path}")
        return

    df = pd.read_csv(scanner_path)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    # -----------------------------
    # Initialize Dash app
    # -----------------------------
    app = Dash(__name__)
    app.title = f"Scanner Dashboard - {file_name}"

    # -----------------------------
    # Layout with sidebar and chart area
    # -----------------------------
    app.layout = html.Div(style={'display': 'flex', 'height': '100vh'}, children=[
        html.Div(style={'width': '20%', 'padding': '20px', 'backgroundColor': '#f4f4f4'}, children=[
            html.H3("Scanner Dashboard", style={'textAlign': 'center'}),
            html.Label("Select Symbol:"),
            dcc.Dropdown(
                id='symbol-dropdown',
                options=[{'label': sym, 'value': sym} for sym in df['symbol'].unique()],
                value=df['symbol'].unique()[0],
                clearable=False
            ),
            html.Hr(),
            html.Div(id='info-box', style={'marginTop': '20px'})
        ]),
        html.Div(style={'width': '80%', 'padding': '20px'}, children=[
            dcc.Graph(id='price-chart')
        ])
    ])

    # -----------------------------
    # Callback to update chart & info box
    # -----------------------------
    @app.callback(
        Output('price-chart', 'figure'),
        Output('info-box', 'children'),
        Input('symbol-dropdown', 'value')
    )
    def update_chart(selected_symbol):
        try:
            # Get signal date from scanner CSV
            row = df[df['symbol'] == selected_symbol]
            if row.empty:
                return go.Figure(), "No signal date found."

            signal_date = row.iloc[0]['date']

            # Fetch symbol_id
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT symbol_id FROM equity_symbols WHERE symbol = ?", (selected_symbol,))
            res = cur.fetchone()
            if not res:
                close_db_connection(conn)
                return go.Figure(), f"{selected_symbol} not found in DB."
            symbol_id = res[0]

            # Fetch price data
            price_df = pd.read_sql("""
                SELECT date, open, high, low, close, adj_close, volume
                FROM equity_price_data
                WHERE symbol_id = ? AND timeframe='1d' AND date >= ?
                ORDER BY date ASC
            """, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
            close_db_connection(conn)

            if price_df.empty:
                return go.Figure(), f"No price data for {selected_symbol} from {signal_date.date()}"

            price_df['date'] = pd.to_datetime(price_df['date'])

            # -----------------------------
            # Create OHLC + Volume chart
            # -----------------------------
            fig = go.Figure()

            # OHLC
            fig.add_trace(go.Candlestick(
                x=price_df['date'],
                open=price_df['open'],
                high=price_df['high'],
                low=price_df['low'],
                close=price_df['close'],
                name='Price',
                increasing_line_color='green',
                decreasing_line_color='red'
            ))

            # Volume as bar
            fig.add_trace(go.Bar(
                x=price_df['date'],
                y=price_df['volume'],
                name='Volume',
                marker_color='blue',
                yaxis='y2',
                opacity=0.3
            ))

            fig.update_layout(
                title=f"{selected_symbol} Price from {signal_date.date()}",
                xaxis_title="Date",
                yaxis_title="Price",
                yaxis=dict(side='left', showgrid=True, title="Price"),
                yaxis2=dict(
                    overlaying='y',
                    side='right',
                    showgrid=False,
                    title="Volume"
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                template="plotly_white",             # <-- change to white template
                paper_bgcolor="white",               # background of the whole page
                plot_bgcolor="white",                # background of chart area
                hovermode="x unified",
                margin=dict(l=50, r=50, t=50, b=50)
            )

            info = html.Div([
                html.P(f"Signal Date: {signal_date.date()}"),
                html.P(f"Symbol: {selected_symbol}"),
                html.P(f"Data Points: {len(price_df)}")
            ])

            return fig, info

        except Exception as e:
            return go.Figure(), f"Error: {e}"

    # -----------------------------
    # Run Dash (blocking)
    # -----------------------------
    app.run(debug=False, port=8050)


# -----------------------------
# CLI entry point
# -----------------------------
if __name__ == "__main__":
    file_name = input("Enter Scanner CSV file name (e.g., Scanner_HM_01Jan2026.csv): ").strip()
    run_scanner_dashboard(file_name)