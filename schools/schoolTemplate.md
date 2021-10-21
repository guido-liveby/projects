# Adding school array to boundaries

## [x] create schema for object in properties of boundary object

```json
{
  "title": "schools",
  "description": "School information associated with a boundary",
  "type": "array",
  "items": {
    "school": {
      "type": "object",
      "properties": {
        "NCESId": {
          "description": "National Center for Education Statisics (NCES) identifier",
          "type": "string"
        },
        "valid": {
          "type": "boolean",
          "description": "Is school valid on boundary",
          "default": true
        },
        "sortOrder": {
          "type": "number",
          "description": "Ascending order number",
          "default": 0
        },
        "schoolName": {
          "type": "string",
          "description": "Name of school"
        },
        "schoolLevel": {
          "type": "string",
          "description": "School level assigned by schooldigger"
        },
        "schoolGradeRange": {
          "type": "string",
          "description": "Grade range (high_grade - low_grade)"
        },
        "editBy": {
          "type": "string",
          "description": "Last edited by"
        },
        "editDate": {
          "type": "string",
          "description": "Last date school was edited"
        },
        "creationBy": {
          "type": "string",
          "description": "Created by",
          "default": "system"
        },
        "creationDate": {
          "type": "string",
          "description": "Date school was added to boundary"
        }
      },
      "required": [
        "NCESId",
        "valid",
        "sortOrder",
        "editBy",
        "editDate",
        "creationBy",
        "creationDate",
        "creationDate"
      ]
    }
  },
  "uniqueItems": true,
  "$comment": [
    "When NCESId is added, 'name','level','gradeRange' automatically fill in from database.",
    "'valid' should be set to true by default and false when the school should not be associated with boundary.",
    "'sortOrder' should be set to 0 and sort order should be order sortorder then schoolname when displayed.",
    "edit and creation fields should be automatically generated when schools array object is edited"
  ]
}
```

## [ ] create schoolsUpdate tool to add schools to each boundary

## [ ] update LBL and LCP apis to read school list off of boundary with sort and valid

## [ ] create admin interface to update school

- [ ] CRUD school array
  - [ ] add new school
  - [ ] edit order
  - [ ] edit valid
- [ ] add school search by name, city, state capabilities
- [ ] add school geospatial search capabilities
