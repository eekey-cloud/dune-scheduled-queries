#!/usr/bin/env python3
"""
Fetch Dune quote>exec data and create scatter plot grouped by venue.
Sends the plot to Slack webhook.
"""

import os
import io
import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
QUERY_ID = 6720746  # Your Dune query ID


def fetch_dune_data():
    """Fetch latest results from Dune query."""
    print(f"Fetching data from Dune query {QUERY_ID}...")
    dune = DuneClient(DUNE_API_KEY)
    query_result = dune.get_latest_result(QUERY_ID)

    # Convert to DataFrame
    rows = query_result.result.rows
    df = pd.DataFrame(rows)
    print(f"Fetched {len(df)} rows")
    return df


def create_scatter_plot(df):
    """Create scatter plot grouped by venue with wide_bps on y-axis."""

    # Ensure block_time is datetime
    df['block_time'] = pd.to_datetime(df['block_time'])

    # Get unique venues for coloring
    venues = df['venue'].unique()
    colors = plt.cm.tab10(range(len(venues)))
    venue_colors = dict(zip(venues, colors))

    # Create figure with appropriate size
    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot each venue separately for legend
    for venue in venues:
        venue_data = df[df['venue'] == venue]
        ax.scatter(
            venue_data['block_time'],
            venue_data['wide_bps'],
            c=[venue_colors[venue]],
            label=venue,
            alpha=0.7,
            s=50,
            edgecolors='white',
            linewidth=0.5
        )

    # Configure y-axis with 0.1 bps increments for clear visualization
    y_min = df['wide_bps'].min()
    y_max = df['wide_bps'].max()

    # Add some padding
    y_range = y_max - y_min
    y_padding = max(y_range * 0.1, 0.5)

    ax.set_ylim(y_min - y_padding, y_max + y_padding)

    # Set y-axis ticks at 0.1 bps intervals if range is reasonable
    if y_range <= 10:
        y_ticks = [i/10 for i in range(int((y_min - y_padding) * 10), int((y_max + y_padding) * 10) + 1)]
        ax.set_yticks(y_ticks)
    elif y_range <= 50:
        y_ticks = [i/2 for i in range(int((y_min - y_padding) * 2), int((y_max + y_padding) * 2) + 1)]
        ax.set_yticks(y_ticks)

    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.axhline(y=0, color='red', linestyle='-', alpha=0.5, linewidth=1)

    # Labels and title
    ax.set_xlabel('Block Time (UTC)', fontsize=12)
    ax.set_ylabel('Wide BPS', fontsize=12)
    ax.set_title(f'Quote vs Exec Spread by Venue\n(Last 24 hours - {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")})', fontsize=14)

    # Legend
    ax.legend(title='Venue', loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=10)

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Tight layout
    plt.tight_layout()

    return fig


def send_to_slack(fig, df):
    """Send the plot to Slack as an image."""

    # Save plot to bytes buffer
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)

    # Save temporarily for upload
    temp_path = '/tmp/quote_exec_scatter.png'
    with open(temp_path, 'wb') as f:
        f.write(buf.getvalue())

    # Calculate summary stats
    summary_stats = df.groupby('venue')['wide_bps'].agg(['mean', 'min', 'max', 'count']).round(2)

    # Create summary text
    summary_text = f"*Quote vs Exec Spread Report*\n"
    summary_text += f"_Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_\n\n"
    summary_text += f"*Total Transactions:* {len(df)}\n\n"
    summary_text += "*Summary by Venue:*\n```\n"
    summary_text += summary_stats.to_string()
    summary_text += "\n```"

    # For Slack webhooks, we need to use Slack's files.upload API for images
    # Since webhooks don't support file uploads, we'll send just the summary
    # and save the image locally or use a different approach

    # Send text summary to Slack
    payload = {
        "text": summary_text,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": summary_text
                }
            }
        ]
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=payload)

    if response.status_code == 200:
        print("Summary sent to Slack successfully!")
    else:
        print(f"Failed to send to Slack: {response.status_code} - {response.text}")

    # Save the plot locally as well
    fig.savefig('quote_exec_scatter.png', dpi=150, bbox_inches='tight')
    print("Plot saved as quote_exec_scatter.png")

    return response.status_code == 200


def main():
    """Main function to fetch data, create plot, and send to Slack."""
    print(f"Starting quote>exec scatter plot job at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # Fetch data from Dune
    df = fetch_dune_data()

    if df.empty:
        print("No data fetched from Dune. Exiting.")
        return

    # Print data info
    print(f"\nData columns: {df.columns.tolist()}")
    print(f"Venues: {df['venue'].unique().tolist()}")
    print(f"Wide BPS range: {df['wide_bps'].min():.2f} to {df['wide_bps'].max():.2f}")

    # Create scatter plot
    fig = create_scatter_plot(df)

    # Send to Slack
    send_to_slack(fig, df)

    plt.close(fig)
    print("\nJob completed!")


if __name__ == "__main__":
    main()
