TRUCKS = [
    {
        "truck_id": f"TRK{str(i).zfill(3)}",
        "driver": f"Driver {i}",
        "route": "Mumbai → Pune",
        "fuel_level": 100,
        "status": "Moving"
    }
    for i in range(1, 51)
]

LOCATIONS = [
    "Mumbai",
    "Thane",
    "Lonavala",
    "Pune"
]