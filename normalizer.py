import re

STOP_WORDS = {"THE"}

def _canonicalize_freeform_name(name: str) -> str:
    """
    Applies generic cleanup so aliases like 'St. Louis' and 'Saint Louis'
    normalize to the same representation.
    """
    if not isinstance(name, str):
        return ""

    cleaned = name.upper()
    cleaned = cleaned.replace("&", " AND ")
    cleaned = re.sub(r'[^A-Z0-9 ]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    replacements = {
        "SAINT": "ST",
        "ST": "ST",
        "SAINTS": "STS",
        "MT.": "MT",
        "MOUNT": "MT",
        "MT": "MT",
    }

    tokens = []
    for token in cleaned.split():
        if token in STOP_WORDS:
            continue
        token = replacements.get(token, token)
        tokens.append(token)

    return " ".join(tokens)

def normalize_team(name):
    """
    Normalizes a team name to a canonical version.
    Example: "Oklahoma City Thunder" -> "OKC"
             "OKC Thunder" -> "OKC"
             "New York Knicks" -> "NYK"
    """
    if not isinstance(name, str):
        return ""
        
    name = name.strip().upper()
    # Remove scores (digits)
    # Example: "ATL HAWKS 102" -> "ATL HAWKS " -> "ATL HAWKS"
    name = re.sub(r'\d+', '', name).strip()
    canonical_name = _canonicalize_freeform_name(name)
    
    # NBA
    mappings = {
        # Custom / City Mappings
        "ATLANTA": "ATL",
        "BOSTON": "BOS",
        "BROOKLYN": "BKN",
        "CHARLOTTE": "CHA",
        "CHICAGO": "CHI",
        "CLEVELAND": "CLE",
        "DALLAS": "DAL",
        "DENVER": "DEN",
        "DETROIT": "DET",
        "GOLDEN STATE": "GSW",
        "HOUSTON": "HOU",
        "INDIANA": "IND",
        "LA CLIPPERS": "LAC",
        "LA LAKERS": "LAL",
        "LOS ANGELES": "LAL", # Context dependent usually, but if just city...
        "LOS ANGELES C": "LAC",
        "LOS ANGELES L": "LAL",
        "MEMPHIS": "MEM",
        "MIAMI": "MIA",
        "MILWAUKEE": "MIL",
        "MINNESOTA": "MIN",
        "NEW ORLEANS": "NOP",
        "NEW YORK": "NYK",
        "OKLAHOMA CITY": "OKC",
        "ORLANDO": "ORL",
        "PHILADELPHIA": "PHI",
        "PHOENIX": "PHX",
        "PORTLAND": "POR",
        "SACRAMENTO": "SAC",
        "SAN ANTONIO": "SAS",
        "TORONTO": "TOR",
        "UTAH": "UTA",
        "WASHINGTON": "WAS",

        # NHL
        "ANAHEIM DUCKS": "ANA",
        "ANAHEIM": "ANA",
        "ARIZONA COYOTES": "ARI", # Relocated to Utah but keep for history
        "BOSTON BRUINS": "BOS",
        "BUFFALO SABRES": "BUF",
        "BUFFALO": "BUF",
        "CALGARY FLAMES": "CGY",
        "CALGARY": "CGY",
        "CAROLINA HURRICANES": "CAR",
        "CAROLINA": "CAR",
        "CHICAGO BLACKHAWKS": "CHI",
        "COLORADO AVALANCHE": "COL",
        "COLORADO": "COL",
        "COLUMBUS BLUE JACKETS": "CBJ",
        "COLUMBUS": "CBJ",
        "DALLAS STARS": "DAL",
        "DETROIT RED WINGS": "DET",
        "EDMONTON OILERS": "EDM",
        "EDMONTON": "EDM",
        "FLORIDA PANTHERS": "FLA",
        "FLORIDA": "FLA",
        "LOS ANGELES KINGS": "LAK",
        "MINNESOTA WILD": "MIN",
        "MONTREAL CANADIENS": "MTL",
        "MONTREAL": "MTL",
        "NASHVILLE PREDATORS": "NSH",
        "NASHVILLE": "NSH",
        "NEW JERSEY DEVILS": "NJ",
        "NEW JERSEY": "NJ",
        "NEW YORK ISLANDERS": "NYI",
        "NEW YORK RANGERS": "NYR",
        "OTTAWA SENATORS": "OTT",
        "OTTAWA": "OTT",
        "PHILADELPHIA FLYERS": "PHI",
        "PITTSBURGH PENGUINS": "PIT",
        "PITTSBURGH": "PIT",
        "SAN JOSE SHARKS": "SJS",
        "SAN JOSE": "SJS",
        "SEATTLE KRAKEN": "SEA",
        "SEATTLE": "SEA",
        "ST. LOUIS BLUES": "STL",
        "ST. LOUIS": "STL",
        "TAMPA BAY LIGHTNING": "TBL",
        "TAMPA BAY": "TBL",
        "TORONTO MAPLE LEAFS": "TOR",
        "UTAH HOCKEY CLUB": "UTA",
        "VANCOUVER CANUCKS": "VAN",
        "VANCOUVER": "VAN",
        "VEGAS GOLDEN KNIGHTS": "VGK",
        "VEGAS": "VGK",
        "WASHINGTON CAPITALS": "WAS",
        "WINNIPEG JETS": "WPG",
        "WINNIPEG": "WPG",

        # NFL
        "ARIZONA CARDINALS": "ARI",
        "ATLANTA FALCONS": "ATL",
        "BALTIMORE RAVENS": "BAL",
        "BALTIMORE": "BAL",
        "BUFFALO BILLS": "BUF",
        "CAROLINA PANTHERS": "CAR",
        "CINCINNATI BENGALS": "CIN",
        "CINCINNATI": "CIN",
        "CLEVELAND BROWNS": "CLE",
        "DALLAS COWBOYS": "DAL",
        "DENVER BRONCOS": "DEN",
        "DETROIT LIONS": "DET",
        "GREEN BAY PACKERS": "GB",
        "GREEN BAY": "GB",
        "HOUSTON TEXANS": "HOU",
        "INDIANAPOLIS COLTS": "IND",
        "INDIANAPOLIS": "IND",
        "JACKSONVILLE JAGUARS": "JAX",
        "JACKSONVILLE": "JAX",
        "KANSAS CITY CHIEFS": "KC",
        "KANSAS CITY": "KC",
        "LAS VEGAS RAIDERS": "LV",
        "LAS VEGAS": "LV",
        "LOS ANGELES CHARGERS": "LAC",
        "LOS ANGELES RAMS": "LAR",
        "MIAMI DOLPHINS": "MIA",
        "MINNESOTA VIKINGS": "MIN",
        "NEW ENGLAND PATRIOTS": "NE",
        "NEW ENGLAND": "NE",
        "NEW ORLEANS SAINTS": "NO",
        "NEW YORK GIANTS": "NYG",
        "NEW YORK JETS": "NYJ",
        "PHILADELPHIA EAGLES": "PHI",
        "PITTSBURGH STEELERS": "PIT",
        "SAN FRANCISCO 49ERS": "SF",
        "SAN FRANCISCO": "SF",
        "SEATTLE SEAHAWKS": "SEA",
        "TAMPA BAY BUCCANEERS": "TB",
        "TENNESSEE TITANS": "TEN",
        "TENNESSEE": "TEN",
        "WASHINGTON COMMANDERS": "WAS",

        # Nicknames / Short
        "THUNDER": "OKC",
        "NUGGETS": "DEN",
        "WARRIORS": "GSW",
        "LAKERS": "LAL",
        "CLIPPERS": "LAC",
        "CELTICS": "BOS",
        "KNICKS": "NYK",
        "NETS": "BKN",
        "RAPTORS": "TOR",
        "76ERS": "PHI",
        "PHI ERS": "PHI",
        "SIXERS": "PHI",
        "HEAT": "MIA",
        "MAGIC": "ORL",
        "HAWKS": "ATL",
        "HORNETS": "CHA",
        "WIZARDS": "WAS",
        "CAVALIERS": "CLE",
        "CAVS": "CLE",
        "PISTONS": "DET",
        "PACERS": "IND",
        "BULLS": "CHI",
        "BUCKS": "MIL",
        "TIMBERWOLVES": "MIN",
        "WOLVES": "MIN",
        "JAZZ": "UTA",
        "TRAIL BLAZERS": "POR",
        "BLAZERS": "POR",
        "KINGS": "SAC",
        "SUNS": "PHX",
        "MAVERICKS": "DAL",
        "MAVS": "DAL",
        "ROCKETS": "HOU",
        "GRIZZLIES": "MEM",
        "PELICANS": "NOP",
        "PELS": "NOP",
        "SPURS": "SAS",

        # NHL Shorts
        "BRUINS": "BOS",
        "SABRES": "BUF",
        "RED WINGS": "DET",
        "FLAMES": "CGY",
        "HURRICANES": "CAR",
        "BLACKHAWKS": "CHI",
        "AVALANCHE": "COL",
        "BLUE JACKETS": "CBJ",
        "STARS": "DAL",
        "OILERS": "EDM",
        "PANTHERS": "FLA",
        "WILD": "MIN",
        "CANADIENS": "MTL",
        "PREDATORS": "NSH",
        "DEVILS": "NJ",
        "ISLANDERS": "NYI",
        "RANGERS": "NYR",
        "SENATORS": "OTT",
        "FLYERS": "PHI",
        "PENGUINS": "PIT",
        "SHARKS": "SJS",
        "KRAKEN": "SEA",
        "BLUES": "STL",
        "LIGHTNING": "TBL",
        "MAPLE LEAFS": "TOR",
        "CANUCKS": "VAN",
        "GOLDEN KNIGHTS": "VGK",
        "OCAPITALS": "WAS",
        "CAPITALS": "WAS",
        "JETS": "WPG",

        # NFL Shorts
        "CARDINALS": "ARI",
        "FALCONS": "ATL",
        "RAVENS": "BAL",
        "BILLS": "BUF",
        "BENGALS": "CIN",
        "BROWNS": "CLE",
        "COWBOYS": "DAL",
        "BRONCOS": "DEN",
        "LIONS": "DET",
        "PACKERS": "GB",
        "TEXANS": "HOU",
        "COLTS": "IND",
        "JAGUARS": "JAX",
        "CHIEFS": "KC",
        "RAIDERS": "LV",
        "CHARGERS": "LAC",
        "RAMS": "LAR",
        "DOLPHINS": "MIA",
        "VIKINGS": "MIN",
        "PATRIOTS": "NE",
        "SAINTS": "NO",
        "GIANTS": "NYG",
        "EAGLES": "PHI",
        "STEELERS": "PIT",
        "49ERS": "SF",
        "SEAHAWKS": "SEA",
        "BUCCANEERS": "TB",
        "TITANS": "TEN",
        "COMMANDERS": "WAS",

        # NBA Full Names
        "OKLAHOMA CITY THUNDER": "OKC",
        "OKC THUNDER": "OKC",
        "OKLAHOMA CITY": "OKC",
        "DENVER NUGGETS": "DEN",
        "GOLDEN STATE WARRIORS": "GSW",
        "LOS ANGELES LAKERS": "LAL",
        "L.A. LAKERS": "LAL",
        "LOS ANGELES CLIPPERS": "LAC",
        "L.A. CLIPPERS": "LAC",
        "BOSTON CELTICS": "BOS",
        "NEW YORK KNICKS": "NYK",
        "NY KNICKS": "NYK",
        "BROOKLYN NETS": "BKN",
        "TORONTO RAPTORS": "TOR",
        "PHILADELPHIA 76ERS": "PHI",
        "MIAMI HEAT": "MIA",
        "ORLANDO MAGIC": "ORL",
        "ATLANTA HAWKS": "ATL",
        "CHARLOTTE HORNETS": "CHA",
        "WASHINGTON WIZARDS": "WAS",
        "CLEVELAND CAVALIERS": "CLE",
        "DETROIT PISTONS": "DET",
        "INDIANA PACERS": "IND",
        "CHICAGO BULLS": "CHI",
        "MILWAUKEE BUCKS": "MIL",
        "MINNESOTA TIMBERWOLVES": "MIN",
        "UTAH JAZZ": "UTA",
        "PORTLAND TRAIL BLAZERS": "POR",
        "SACRAMENTO KINGS": "SAC",
        "PHOENIX SUNS": "PHX",
        "DALLAS MAVERICKS": "DAL",
        "HOUSTON ROCKETS": "HOU",
        "MEMPHIS GRIZZLIES": "MEM",
        "NEW ORLEANS PELICANS": "NOP",
        "SAN ANTONIO SPURS": "SAS",
    }
    
    # Check exact match
    if name in mappings:
        return mappings[name]
    if canonical_name in mappings:
        return mappings[canonical_name]
        
    # Check partials
    for rich_name, code in mappings.items():
        if rich_name in name:
            return code
        if canonical_name and rich_name in canonical_name:
            return code
            
    # Fallback: Use first 3 letters? No, dangerous.
    # Return cleaned name
    return canonical_name or name

def normalize_event(event_str):
    """
    Attempts to create a canonical event string.
    Inputs like: "OKC Thunder vs DEN Nuggets"
    Output: "OKC vs DEN"
    """
    if " vs. " in event_str:
        parts = event_str.split(" vs. ")
    elif " vs " in event_str:
        parts = event_str.split(" vs ")
    elif " @ " in event_str:
        parts = event_str.split(" @ ")
    else:
        return event_str
        
    if len(parts) == 2:
        t1 = normalize_team(parts[0])
        t2 = normalize_team(parts[1])
        # Sort them to ensure A vs B == B vs A equality
        teams = sorted([t1, t2])
        return f"{teams[0]} vs {teams[1]}"
        
    return event_str
