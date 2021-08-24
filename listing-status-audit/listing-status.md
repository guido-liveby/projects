# [Listing Status Review](https://app.asana.com/0/1200409910523705/1200604023781607)

- [ ] Convert the following information into a spreadsheet: https://docs.google.com/document/d/1qUPSv-HclHLAWHZKcN0KdRck-GY2gDy_71VyZ1_Wk0g/edit?ts=60ee1b85 vendor is ID given to the regional real estate association aka MLS.
  - You can cross-reference IDs with actual MLS names using this spreadheet
- [ ] Identify cases where the status and mls values do not match. Status is the standardized field, mls is the raw field directly from the data provider. If the casing of text does not match, that is still considered a match.
- [ ] Once matches are identified consult with PO
- [ ] Research the correct mapping and make an organized list of requested mapping changes that we will send to our Standardizing Service (SimplyRETS).

- select

  ```js
  db.properties.find(
    {
      vendor: 'armls',
      'mls.status': 'ActiveUnderContract'
    },
    {
      'mls.status': 1,
      'mls.statusText': 1,
      listingId: 1,
      _id: 0
    }
  )
  ```