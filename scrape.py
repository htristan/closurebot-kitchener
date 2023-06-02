import requests
from bs4 import BeautifulSoup
import discord
from discord import Embed
from datetime import date
import boto3
import re
import os

# Discord bot token and channel ID
DISCORD_TOKEN = os.environ['DISCORD_TOKEN']
AWS_ACCESS_KEY_ID = os.environ['AWS_DB_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_DB_SECRET_ACCESS_KEY']
CHANNEL_ID = '1111507316274114651'

# URL of the City of Kitchener road closures page
url = 'https://app2.kitchener.ca/roadclosures/'

dynamodb = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
dbTable = dynamodb.Table("ClosureBotDB")


def scrape_kitchener_closures():
    # Send a GET request to the road closures page
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the road closures on the page
        caption = soup.find('caption', string=lambda text: text and 'New Road Closures' in text)
        caption2 = soup.find('caption', string=lambda text: text and 'Emergency Road Closures' in text)
        if caption is None and caption2 is None:
            print("Caption elements not found.")
            return  # or continue with the rest of the code if applicable
        if caption != None:
            table = caption.find_parent('table', class_='datatable')

            table_body = table.find('tbody')

            rows = table_body.find_all('tr')

            for row in rows:
                if row.find('a', href='#top'):
                    continue
                # Extract closure details
                columns = row.find_all('td')
                road_name = columns[0].text.strip()
                from_to = columns[1].text.strip()
                closure_info = columns[2].text.strip()
                date_match = re.search(r'Date:\s+([\w-]+)\s+to\s+([\w-]+)', closure_info)
                closure_date = f"{date_match.group(1)} to {date_match.group(2)}" if date_match else "Unknown"

                if check_closure_exists('Kitchener', road_name, closure_date):
                    continue
                else:
                    dbTable.put_item(
                        Item={
                        'CityArea': 'Kitchener',
                        'RoadName': road_name,
                        'FromTo': from_to,
                        'ClosureDate': closure_date,
                        'ClosureInfo': closure_info
                    }
                )

                # Notify the closure on Discord
                notify_discord(road_name, from_to, closure_info)
        if caption2 != None:
            table = caption2.find_parent('table', class_='datatable')

            table_body = table.find('tbody')

            rows = table_body.find_all('tr')

            for row in rows:
                if row.find('a', href='#top'):
                    continue
                # Extract closure details
                columns = row.find_all('td')
                road_name = columns[0].text.strip()
                from_to = columns[1].text.strip()
                closure_info = columns[2].text.strip()
                #emergency clsoure - set date to today - normally there is no date there
                closure_date = date.today().strftime("%Y-%b-%d")

                if check_closure_exists('Kitchener', road_name, closure_date):
                    continue
                else:
                    dbTable.put_item(
                        Item={
                        'CityArea': 'Kitchener',
                        'RoadName': road_name,
                        'FromTo': from_to,
                        'ClosureDate': closure_date,
                        'ClosureInfo': closure_info
                    }
                )

                # Notify the closure on Discord
                notify_discord(road_name, from_to, closure_info)
    else:
        print('Failed to scrape road closures.')

def check_closure_exists(city_area, road_name, closure_date):
    dbResponse = dbTable.get_item(
                Key={
                    'CityArea': city_area,
                    'RoadName': road_name,
                }
            )
    item = dbResponse.get('Item')
    if item:
        existing_closure_date = item.get('ClosureDate')
        if existing_closure_date == closure_date:
            return True
    return False

def notify_discord(road_name, from_to, closure_info):
    # Create a Discord bot client
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        # Find the target channel by ID
        channel = client.get_channel(int(CHANNEL_ID))
        if channel:
            embed = Embed(title="Kitchener Road Closure Update", color=discord.Color.red())
            try:
                closure_reason_match = re.search(r'Reason:\s+(.+)\s+', closure_info)
                closure_reason = closure_reason_match.group(1) if closure_reason_match else None
                if closure_reason is None:
                    raise IndexError("Closure reason not found")

                closure_date_match = re.search(r'Date:\s+([\w-]+)\s+to\s+([\w-]+)', closure_info)
                closure_date = f"{closure_date_match.group(1)} to {closure_date_match.group(2)}" if closure_date_match else None
                if closure_date is None:
                    raise IndexError("Closure date not found")

                closure_details_match = re.search(r'Details:\s+(.+)\s+', closure_info)
                closure_details = closure_details_match.group(1) if closure_details_match else None
                if closure_details is None:
                    raise IndexError("Closure details not found")

                closure_contact_match = re.search(r'Contact:\s+(.+)', closure_info)
                closure_contact = closure_contact_match.group(1) if closure_contact_match else None
                if closure_contact is None:
                    raise IndexError("Clsoure contact not found")
                # Create a rich embed for the closure notification
                embed.add_field(name="Road", value=road_name, inline=False)
                embed.add_field(name="From/To", value=from_to, inline=False)
                embed.add_field(name="Reason", value=closure_reason, inline=False)
                embed.add_field(name="Date", value=closure_date, inline=False)
                embed.add_field(name="Details", value=closure_details, inline=False)
                embed.add_field(name="Contact", value=closure_contact, inline=False)
            except IndexError:
                embed.add_field(name="Details", value=f"Road: {road_name}\nFrom/To: {from_to}\nClosure Info: {closure_info}")

            # Send the closure notification
            await channel.send(embed=embed)
        else:
            print(f'Failed to find the Discord channel with ID: {CHANNEL_ID}')
        await client.close()

    client.run(DISCORD_TOKEN)

def lambda_handler(event, context):
    scrape_kitchener_closures()

def send_test_event():
    notify_discord('test road name', 'EARL ST TO BELMONT AVE W', "Reason: Special Event\
            Date: 2023-May-26 to 2023-May-27\
            Details: 2 day closure local access only\
            Contact: Stephanie Brasseur 519-741-2200 ext. 7373")
    notify_discord('test road name', 'Earl st test',"reason: asdfasdflkjasdfoijqweoimsadoifjs")