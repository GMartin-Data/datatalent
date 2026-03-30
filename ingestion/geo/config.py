BASE_URL = "https://geo.api.gouv.fr"

RESOURCES = {
    "regions": "code,nom,zone",
    "departements": "code,nom,codeRegion,zone",
    "communes": "code,nom,codesPostaux,codeDepartement,codeRegion,"
    + "population,centre,surface",
}
