#requires BeautifulSoup 4 and lxml
#Install method:
#pip install beautifulsoup4
#pip install lxml


from bs4 import BeautifulSoup

''' example bs4 implementation based on http://www.w3schools.com/xml/schema_example.asp

markup = """
<?xml version="1.0" encoding="UTF-8"?>

<shiporder orderid="889923"
  <orderperson>John Smith</orderperson>
  <shipto>
    <name>Ola Nordmann</name>
    <address>Langgt 23</address>
    <city>4000 Stavanger</city>
    <country>Norway</country>
  </shipto>
  <item>
    <title>Empire Strikes Back</title>
    <note>Special Edition</note>
    <quantity>1</quantity>
    <price>10.90</price>
  </item>
  <item>
    <title>Hide your heart</title>
    <quantity>1</quantity>
    <price>9.90</price>
  </item>
</shiporder>

<!-- example xml file for beautifulsoup -->
"""
soup = BeautifulSoup(markup)

soup.prettify()

deliverycity = soup.shiporder.city.string
deliverycountry = soup.shiporder.country.string

print deliverycity
print deliverycountry

'''

#process example for our image metadata
#1. Determine metadata location (step in this file, or before it?)
#2. read in xml file
#3. Parse, obtaining specific details
   # Solar Azimuth Angle : MEANSUNAZ
    # Solar Zenith Angle : 90 - MEANSUNEL  #confirm it's in degrees first
    ## Solar zentih angle is complementary to solar elevation angle (they add to 90 degrees)
    ## Since these two angles are complementary, the cosine of either one of them equals the sine of the other.
    ## test against https://www.esrl.noaa.gov/gmd/grad/solcalc/azel.html
#4. convert to radians ()





