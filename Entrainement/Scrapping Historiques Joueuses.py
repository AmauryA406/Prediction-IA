"""
Script pour récupérer les données HISTORIQUES des 120 meilleures joueuses WTA
Objectif: Créer un dataset d'entraînement pour le modèle ML
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from pathlib import Path
import re
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping_training.log'),
        logging.StreamHandler()
    ]
)

class TrainingDataScraper:
    """
    Scrape les données historiques complètes pour l'entraînement
    """
    
    def __init__(self, output_dir='data/raw'):
        self.base_url = "https://www.tennisabstract.com/cgi-bin/wplayer.cgi"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.delay = 2.5  # secondes entre requêtes
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def get_player_id(self, first_name, last_name):
        """
        Génère l'ID Tennis Abstract
        Ex: Aryna Sabalenka -> ASabalenka
        """
        first = re.sub(r'[^a-zA-Z]', '', first_name)
        last = re.sub(r'[^a-zA-Z]', '', last_name)
        
        if first and last:
            return f"{first[0]}{last}"
        return None
    
    def scrape_player(self, player_id, player_name):
        """
        Scrape TOUS les matchs historiques d'une joueuse
        """
        url = f"{self.base_url}?player={player_id}"
        
        logging.info(f"Scraping: {player_name} ({player_id})")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            if "No such player" in response.text or len(response.text) < 500:
                logging.warning(f"  ⚠ Page invalide")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            matches = self.extract_matches(soup, player_name)
            
            if matches:
                logging.info(f"  ✓ {len(matches)} matchs trouvés")
                return matches
            else:
                logging.warning(f"  ⚠ Aucun match")
                return None
                
        except Exception as e:
            logging.error(f"  ✗ Erreur: {e}")
            return None
    
    def extract_matches(self, soup, player_name):
        """
        Extrait tous les matchs depuis la page HTML
        """
        matches = []
        
        # Chercher tous les tableaux
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            if len(rows) < 2:
                continue
            
            # Header
            header_row = rows[0]
            headers = []
            for cell in header_row.find_all(['th', 'td']):
                text = cell.get_text(strip=True)
                headers.append(text)
            
            # Vérifier que c'est un tableau de matchs
            header_str = ' '.join(headers).lower()
            if not any(kw in header_str for kw in ['opponent', 'score', 'tournament', 'date', 'surface']):
                continue
            
            # Parser les matchs
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue
                
                match = self.parse_match(cols, headers, player_name)
                if match:
                    matches.append(match)
        
        return matches
    
    def parse_match(self, cols, headers, player_name):
        """
        Parse une ligne de match
        """
        match = {'player': player_name}
        
        for i, header in enumerate(headers):
            if i >= len(cols):
                break
            
            h = header.lower().strip()
            val = cols[i].get_text(strip=True)
            
            if not val or val == '-':
                continue
            
            # Mapping des colonnes
            if 'date' in h:
                match['date'] = val
            elif 'year' in h:
                match['year'] = val
            elif 'tournament' in h or 'event' in h:
                match['tournament'] = val
            elif h in ['surface', 'sfc', 'surf']:
                match['surface'] = val
            elif h in ['round', 'rd', 'rnd']:
                match['round'] = val
            elif h in ['rank', 'rk']:
                match['rk'] = val
            elif h in ['opp', 'opponent']:
                match['opponent_raw'] = val
            elif h in ['ork', 'opp rk', 'opp rank', 'vrk']:
                match['vrk'] = val
            elif 'score' in h:
                match['score'] = val
            elif 'w/l' in h or 'result' in h:
                match['result'] = val
            elif 'dr' in h:
                match['dr'] = val
            # Stats de jeu
            elif 'ace' in h and '%' in h:
                match['ace%'] = val
            elif 'df' in h and '%' in h:
                match['df%'] = val
            elif '1stin' in h or '1st in' in h:
                match['1stin'] = val
            elif '1stw' in h or '1st%' in h or '1st won' in h:
                match['1st%'] = val
            elif '2ndw' in h or '2nd%' in h or '2nd won' in h:
                match['2nd%'] = val
            elif 'bpsv' in h or 'bp sav' in h:
                match['bpsvd'] = val
            elif 'time' in h or 'min' in h:
                match['time'] = val
        
        return match if len(match) > 3 else None
    
    def scrape_all(self, players_csv='top_120_wta_players.csv'):
        """
        Scrape toutes les joueuses
        """
        # Chercher le fichier dans plusieurs emplacements
        possible_paths = [
            players_csv,
            os.path.join('Entrainement', players_csv),
            os.path.join(os.path.dirname(__file__), players_csv),
            os.path.join(os.path.dirname(__file__), '..', players_csv)
        ]

        csv_path = None
        for path in possible_paths:
            if os.path.exists(path):
                csv_path = path
                break

        if not csv_path:
            logging.error(f"Fichier {players_csv} introuvable dans les emplacements: {possible_paths}")
            return
        
        df = pd.read_csv(csv_path)
        total = len(df)
        
        print("="*70)
        print(f"🎾 SCRAPING DONNÉES D'ENTRAÎNEMENT - {total} JOUEUSES")
        print("="*70)
        print(f"📁 Destination: {self.output_dir.absolute()}")
        print(f"⏱️  Temps estimé: {(total * self.delay) / 60:.0f} minutes")
        print("="*70)
        print()
        
        success = 0
        fail = 0
        
        for idx, row in df.iterrows():
            num = idx + 1
            rank = row['rank']
            name = row['full_name']
            first = row['first_name']
            last = row['last_name']
            
            print(f"[{num}/{total}] #{rank} {name}")
            
            player_id = self.get_player_id(first, last)
            if not player_id:
                logging.warning(f"  ✗ ID invalide")
                fail += 1
                continue
            
            # Scrape
            matches = self.scrape_player(player_id, name)
            
            if matches and len(matches) > 0:
                # Sauvegarder
                df_matches = pd.DataFrame(matches)
                
                safe_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')
                filename = self.output_dir / f"{rank:03d}_{safe_name}.csv"
                
                df_matches.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"  💾 {filename.name} ({len(matches)} matchs)")
                success += 1
            else:
                print(f"  ✗ Échec")
                fail += 1
            
            # Pause
            if num < total:
                time.sleep(self.delay)
        
        print()
        print("="*70)
        print("📊 RÉSUMÉ")
        print("="*70)
        print(f"✅ Réussis: {success}/{total}")
        print(f"❌ Échecs:  {fail}/{total}")
        print(f"📁 Fichiers bruts sauvegardés dans: {self.output_dir.absolute()}")
        print("="*70)
        print()
        print("ℹ️  Le dataset combiné sera créé en Phase 2 après nettoyage des données")
        print("="*70)
    
    def combine_all(self):
        """
        Combine tous les CSV en un seul fichier
        """
        print()
        print("🔄 Création du dataset combiné...")
        
        files = list(self.output_dir.glob("*.csv"))
        
        if not files:
            print("⚠ Aucun fichier trouvé!")
            return
        
        dfs = []
        for f in files:
            try:
                df = pd.read_csv(f)
                dfs.append(df)
            except Exception as e:
                logging.warning(f"Erreur {f.name}: {e}")
        
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            
            # Filtrer uniquement surface dure
            if 'surface' in combined.columns:
                before = len(combined)
                combined = combined[
                    combined['surface'].str.lower().str.contains('hard', na=False)
                ]
                print(f"🎾 Filtre Hard Court: {len(combined):,}/{before:,} matchs")
            
            # Sauvegarder
            output = Path('data/training_data.csv')
            output.parent.mkdir(exist_ok=True)
            combined.to_csv(output, index=False, encoding='utf-8-sig')
            
            print(f"✅ Dataset créé: {output}")
            print(f"   📊 {len(combined):,} matchs")
            print(f"   👥 {combined['player'].nunique()} joueuses")
            
            # Stats
            if 'date' in combined.columns:
                try:
                    combined['date_parsed'] = pd.to_datetime(combined['date'], errors='coerce')
                    print(f"   📅 Période: {combined['date_parsed'].min().year} - {combined['date_parsed'].max().year}")
                except:
                    pass
            
            print()
            print("🎯 Prêt pour Phase 2: Préparation des données!")


def main():
    print("="*70)
    print("🎾 SCRAPER DONNÉES D'ENTRAÎNEMENT WTA")
    print("="*70)
    print()
    print("Ce script récupère TOUS les matchs historiques")
    print("des 120 meilleures joueuses depuis Tennis Abstract.")
    print()
    
    response = input("Lancer le scraping? (o/n): ").lower()
    
    if response != 'o':
        print("Annulé")
        return
    
    print()
    
    scraper = TrainingDataScraper()
    scraper.scrape_all()
    
    print()
    print("✅ TERMINÉ!")


if __name__ == "__main__":
    main()