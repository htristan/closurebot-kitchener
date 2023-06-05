import requests
from bs4 import BeautifulSoup
import discord
from discord import Embed
from datetime import date
import boto3
from boto3.dynamodb.conditions import Key
import re
import os

# Discord bot token and channel ID
DISCORD_TOKEN = os.environ['DISCORD_TOKEN']
AWS_ACCESS_KEY_ID = os.environ['AWS_DB_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_DB_SECRET_ACCESS_KEY']
CHANNEL_ID = '1111507316274114651'

# URL of the City of Kitchener road closures page
url_kitchener = 'https://app2.kitchener.ca/roadclosures/'
url_hamilton = 'https://www.hamilton.ca/home-neighbourhood/getting-around/driving-traffic/road-closures'

dynamodb = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
dbTable = dynamodb.Table("ClosureBotDB")

def scrape_hamilton_closures():
    # Send a GET request to the road closures page
    response = requests.get(url_hamilton)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        #headers we want
        match_header = 'Start Date'
        
        tables = soup.find_all('table')

        for table in tables:
            headers = [th.get_text(strip=True) for th in table.find_all('th')]

            if match_header in headers:
                #we found the table we want
                table_body = table.find('tbody')

                rows = table_body.find_all('tr')

                for row in rows:
                    # Extract closure details
                    columns = row.find_all('td')
                    road_name = columns[2].text.strip()
                    closure_info = columns[3].text.strip()
                    closure_date = f"{columns[0].text.strip()} to {columns[1].text.strip()}"

                    needNotify = False
                    exists, status = check_closure_exists('Hamilton', road_name, closure_date)
                    if exists:
                        if status == "Append":
                            dbTable.update_item(
                                Key={'CityArea': 'Hamilton',
                                'RoadName': road_name,
                                },
                            UpdateExpression="SET ClosureDate = list_append(ClosureDate, :newdate)",
                            ExpressionAttributeValues={
                                ':newdate': [closure_date]
                            },
                            ReturnValues="UPDATED_NEW"
                        )
                        needNotify = True
                        # if status is "existing, we do nothing"
                        continue
                    else:
                        dbTable.put_item(
                            Item={
                            'CityArea': 'Hamilton',
                            'RoadName': road_name,
                            'ClosureDate': [closure_date],
                            'ClosureInfo': closure_info
                            }
                        )
                        needNotify = True
                    if needNotify:
                        #Notify the closure on Discord
                        notify_discord('Hamilton', road_name, closure_info, closure_dates=closure_date, from_to=None)
    else:
        print('Failed to scrape road closures.')

def scrape_kitchener_closures():
    # Send a GET request to the road closures page
    response = requests.get(url_kitchener)
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

                needNotify = False
                exists, status = check_closure_exists('Kitchener', road_name, closure_date)
                if exists:
                    if status == "Append":
                            dbTable.update_item(
                                Key={'CityArea': 'Kitchener',
                                'RoadName': road_name,
                                },
                            UpdateExpression="SET ClosureDate = list_append(ClosureDate, :newdate)",
                            ExpressionAttributeValues={
                                ':newdate': [closure_date]
                            },
                            ReturnValues="UPDATED_NEW"
                        )
                    needNotify = True
                    # if status is "existing, we do nothing"
                    continue
                else:
                    dbTable.put_item(
                        Item={
                        'CityArea': 'Kitchener',
                        'RoadName': road_name,
                        'FromTo': from_to,
                        'ClosureDate': [closure_date],
                        'ClosureInfo': closure_info
                        }
                    )
                    needNotify = True

                # Notify the closure on Discord
                notify_discord(road_name, closure_info, from_to, closure_date)
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
                        'ClosureDate': [closure_date],
                        'ClosureInfo': closure_info
                    }
                )

                # Notify the closure on Discord
                notify_discord('Kitchener', road_name, closure_info, from_to)
    else:
        print('Failed to scrape road closures.')

def check_closure_exists(city_area, road_name, closure_date):
    dbResponse = dbTable.query(
        KeyConditionExpression=Key('CityArea').eq(city_area) & Key('RoadName').eq(road_name)
    )
    items = dbResponse.get('Items')
    if items:
        for item in items:
            existing_closure_dates = item.get('ClosureDate')
            if isinstance(existing_closure_dates, list):
                if closure_date in existing_closure_dates:
                    return True, "Existing"
                else:
                    return True, "Append"
            else:
                if existing_closure_dates == closure_date:
                    return True, "Existing"
    return False, "New"

def notify_discord(city_name, road_name, closure_info, from_to=None, closure_dates=None):
    # Create a Discord bot client
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        # Find the target channel by ID
        channel = client.get_channel(int(CHANNEL_ID))
        if channel:
            embed = Embed(title=f"{city_name} Road Closure Update", color=discord.Color.red())
            if city_name == 'Kitchener':
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
            else:
                embed.add_field(name="Road", value=road_name, inline=False)
                embed.add_field(name="Details", value=closure_info, inline=False)
                if from_to:
                    embed.add_field(name="From/To", value=from_to, inline=False)
                if closure_dates:
                    embed.add_field(name="Date", value=closure_dates, inline=False)

            # Send the closure notification
            await channel.send(embed=embed)
        else:
            print(f'Failed to find the Discord channel with ID: {CHANNEL_ID}')
        await client.close()

    client.run(DISCORD_TOKEN)

def lambda_handler(event, context):
    scrape_kitchener_closures()
    scrape_hamilton_closures()

def send_test_event():
    notify_discord('Kitchener', 'test road name', "Reason: Special Event\
            Date: 2023-May-26 to 2023-May-27\
            Details: 2 day closure local access only\
            Contact: Stephanie Brasseur 519-741-2200 ext. 7373", 'EARL ST TO BELMONT AVE W')
    notify_discord('Kithcener', 'test road name',"reason: asdfasdflkjasdfoijqweoimsadoifjs", 'Earl st test')