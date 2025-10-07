import pandas as pd
import random
from datetime import datetime, timedelta

def generate_sample_data():
    machines = ['A001', 'A002', 'A003', 'B001', 'B002', 'C001']
    shifts = ['Matin', 'Après-midi', 'Nuit']
    operators = ['Jean', 'Marie', 'Pierre', 'Sophie', 'Luc', 'Anna']

    data = []
    for _ in range(1000):
        date = datetime.now() - timedelta(days=random.randint(0, 30))
        data.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Machine': random.choice(machines),
            'Shift': random.choice(shifts),
            'Operateur': random.choice(operators),
            'Quantité_produite': random.randint(800, 1200),
            'Défauts': random.randint(0, 50),
            'Temps_arret(min)': random.randint(0, 120),
            'Maintenance_preventive': random.choice([True, False]),
            'Temperature': round(random.uniform(18.0, 35.0), 1),
            'Humidite': round(random.uniform(40.0, 80.0), 1)
        })

    df = pd.DataFrame(data)
    df.to_csv("data/production_enhanced.csv", index=False)
    return df
