import json
import boto3
import os
import codecs
import sys
import urllib3
import io

http = urllib3.PoolManager()

dynamodb = boto3.resource('dynamodb')

client = boto3.client('dynamodb')

tableName = os.environ['table']

p1 = client.get_item(
    Key={
        'name': {
            'S': 'XSRF-TOKEN',
        },
    },
    TableName='lodge104-keys',
)
p2 = client.get_item(
    Key={
        'name': {
            'S': 'ai_session',
        },
    },
    TableName='lodge104-keys',
)
p3 = client.get_item(
    Key={
        'name': {
            'S': 'OA.LM.Lodge.Auth',
        },
    },
    TableName='lodge104-keys',
) 
p4 = client.get_item(
    Key={
        'name': {
            'S': 'ai_user',
        },
    },
    TableName='lodge104-keys',
) 
url = "https://lodgemaster-client.oa-bsa.org/odata/MemberGrid?%24orderby=BsaPersonId%20desc&%24skip=10&%24top=10000&%24select=Positions%2CEmailPrimaryBounced%2CPhonePrimaryBounced%2CBrotherhoodDate%2CBrotherhoodEligibleDate%2CBsaRegistrationActive%2CBsaRegistrationOverridden%2CBsaPersonId%2CBsaLastCheckDate%2CBsaLastCheckStatus%2CChapter%2CAddressPrimaryCity%2CDateOfBirth%2CDateOfDeath%2CDistrict%2CDuesInputDate%2CDuesPaidDate%2CElectionDate%2CEmailPrimaryAddress%2CEmailPrimaryType%2CEthnicBackground%2CFirstName%2CFullName%2CGender%2CEmailHomeBounced%2CEmailHomeAddress%2CLastName%2CLastUpdatedDate%2CPositionLec%2CLevel%2CEmailLodgeAddress%2CDuesYear%2CMiddleName%2CPhoneMobileNumber%2CNickname%2CEmailPrimaryNoContact%2CAddressPrimaryNoContact%2COrdealDate%2CCustomEmailParent2Address%2CEmailParentAddress%2CPhonePrimaryNumber%2CPhonePrimaryType%2CPortalAccount%2CAddressPrimaryState%2CAddressPrimaryStreet1%2CAddressPrimaryStreet2%2CSuffix%2CUnit%2CUnitCity%2CUnitDesignation%2CUnitNumber%2CUnitRepresentative%2CUnitType%2CVigilEligibleDate%2CVigilInductionDate%2CVigilNameEnglish%2CVigilNameTranslated%2CYouthProtectionCompliant%2CYouthProtectionDate%2CYouthProtectionOverridden%2CYouthAdult%2CAddressPrimaryZipCode%2COalmId&%24filter=BsaPersonId%20ne%20null&duesIncludeYears=2023%2C2049%2C2048%2C2047%2C2046%2C2045%2C2044%2C2043%2C2042%2C2041%2C2040%2C2039%2C2038%2C2037%2C2036%2C2035%2C2034%2C2033%2C2032%2C2031%2C2030%2C2029%2C2028%2C2027%2C2026%2C2025%2C2024%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014%2C2013%2C2012%2C2011%2C2010%2C2009%2C2008%2C2007%2C2006%2C2005%2C2004%2C2003%2C2002%2C2001%2C2000%2C1999%2C1998%2C1997%2C1993%2C1982%2C1981%2C1974"

print(p1)

headers = {
'Cookie': p1['Item']['name']['S'] + '=' + p1['Item']['value']['S'] + '; ' + p2['Item']['name']['S'] + '=' + p2['Item']['value']['S'] + '; ' + p3['Item']['name']['S'] + '=' + p3['Item']['value']['S'] + '; ' + p4['Item']['name']['S'] + '=' + p4['Item']['value']['S'] + ';',
'X-Xsrf-Token': p1['Item']['value']['S']
}

def lambda_handler(event, context):
   #get() does not store in memory
   try:
      table = dynamodb.Table(tableName)
   except Exception as error:
      print(error)
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   #DictReader is a generator; not stored in memory
   obj = http.request('GET', url, headers=headers)
   body = obj.data
   content = json.loads(body)
   with table.batch_writer() as batch:
       for item in content['value']:
            print(item)
            batch.put_item(Item=item)