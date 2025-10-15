import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_wta_rankings(max_players=120):
    """
    Scrape les rankings WTA depuis Tennis Abstract
    """
    url = "https://tennisabstract.com/reports/wtaRankings.html"
    
    print(f"Récupération des données depuis {url}...")
    
    try:
        # Récupérer la page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parser le HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Trouver le tableau des rankings
        table = soup.find('table', {'id': 'reportable'})
        
        if not table:
            # Essayer de trouver n'importe quel tableau
            table = soup.find('table')
            
        if not table:
            print("Erreur : Tableau non trouvé")
            return None
        
        # Extraire les données
        players_data = []
        rows = table.find_all('tr')
        
        for i, row in enumerate(rows):
            if i == 0:  # Skip header
                continue
            if len(players_data) >= max_players:
                break
                
            cols = row.find_all('td')
            
            if len(cols) >= 3:
                rank_text = cols[0].text.strip()
                player_name = cols[1].text.strip()
                country = cols[2].text.strip()
                
                # Extraire le rang (peut contenir des parenthèses)
                rank = rank_text.replace('(', '').replace(')', '')
                
                if not player_name or player_name == '':
                    continue
                
                # Séparer prénom et nom (format: Prénom(s) NOM)
                name_parts = player_name.split()
                if len(name_parts) >= 2:
                    last_name = name_parts[-1]
                    first_name = ' '.join(name_parts[:-1])
                else:
                    first_name = player_name
                    last_name = ""
                
                players_data.append({
                    'rank': int(rank) if rank.isdigit() else rank,
                    'first_name': first_name,
                    'last_name': last_name,
                    'full_name': player_name,
                    'country': country
                })
        
        # Créer le DataFrame
        df = pd.DataFrame(players_data)
        
        print(f"\n✓ {len(df)} joueuses récupérées avec succès")
        
        return df
    
    except Exception as e:
        print(f"Erreur lors du scraping : {e}")
        return None


def save_to_csv(df, filename='top_120_wta_players.csv'):
    """
    Sauvegarde le DataFrame en CSV
    """
    if df is not None:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n✓ Fichier sauvegardé : {filename}")
        print(f"\nAperçu des données :")
        print(df.head(10))
        print(f"\n...")
        print(df.tail(5))
    else:
        print("Aucune donnée à sauvegarder")


if __name__ == "__main__":
    # Scraper les données
    df = scrape_wta_rankings(max_players=120)
    
    # Sauvegarder en CSV
    if df is not None:
        save_to_csv(df)
        
        # Statistiques
        print(f"\n" + "="*50)
        print(f"STATISTIQUES")
        print(f"="*50)
        print(f"Nombre total de joueuses : {len(df)}")
        print(f"Pays représentés : {df['country'].nunique()}")
        print(f"\nTop 10 pays par nombre de joueuses :")
        print(df['country'].value_counts().head(10))