from enum import Enum


class AstralTime(Enum):

    DAWN_ASTRO = "dawn_astro"
    DAWN_NAUTICAL = "dawn_nautical"
    DAWN_CIVIL = "dawn_civil"

    SUNRISE = "sunrise"
    NOON = "noon"
    SUNSET = "sunset"

    DUSK_CIVIL = "dusk_civil"
    DUSK_NAUTICAL = "dusk_nautical"
    DUSK_ASTRO = "dusk_astro"

    MIDNIGHT = "midnight"


class AstralTimesConfKey:

    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    ELEVATION = "elevation"


ASTRAL_TIMES_JSONSCHEMA = {
    "additionalProperties": False,
    "required": [AstralTimesConfKey.LATITUDE, AstralTimesConfKey.LONGITUDE, AstralTimesConfKey.ELEVATION],
    "type": "object",
    "properties": {
        AstralTimesConfKey.LATITUDE: {"type": "number", "minimum": -90, "maximum": 90},
        AstralTimesConfKey.LONGITUDE: {"type": "number", "minimum": -180, "maximum": 180},
        AstralTimesConfKey.ELEVATION: {
            "type": "number",
            "minimum": -1000,
            "maximum": 9000,
            "description": "in meters",
        },
    },
    "description": "A concrete location is necessary to use the astral time service. You don't need to configured it, "
                   "but this will lead to errors in case you use it.",
}
