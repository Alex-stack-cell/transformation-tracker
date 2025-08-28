import pandas as pd
from pathlib import Path


def explore_parquet_files():
    """Explore tous les fichiers parquet dans data/raw/"""
    raw_dir = Path("data/raw")

    print("🔍 EXPLORATION DES DONNÉES COLLECTÉES\n")
    print("="*50)

    # Liste des fichiers parquet
    parquet_files = list(raw_dir.glob("*.parquet"))

    if not parquet_files:
        print("❌ Aucun fichier parquet trouvé dans data/raw/")
        return

    for file_path in parquet_files:
        print(f"\n📄 FICHIER: {file_path.name}")
        print("-" * 40)

        try:
            # Charger le fichier
            df = pd.read_parquet(file_path)

            # Infos générales
            print(
                f"📊 Dimensions: {df.shape[0]} lignes × {df.shape[1]} colonnes")
            print(
                f"📅 Collecté le: {df['collection_timestamp'].iloc[0] if 'collection_timestamp' in df.columns else 'N/A'}")

            # Colonnes
            print(f"\n🏷️  Colonnes:")
            for i, col in enumerate(df.columns, 1):
                dtype = str(df[col].dtype)
                print(f"  {i:2d}. {col} ({dtype})")

            # Aperçu des données
            print(f"\n👀 Aperçu des données:")
            print(df.head(3).to_string())  # type: ignore

            # Stats rapides pour colonnes numériques
            numeric_cols = df.select_dtypes(
                include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                print(f"\n📈 Statistiques rapides:")
                for col in numeric_cols[:3]:  # Limiter à 3 colonnes
                    mean_val = df[col].mean()
                    print(f"  {col}: moyenne = {mean_val:.2f}")

        except Exception as e:
            print(f"❌ Erreur lors de la lecture: {e}")

    print("\n" + "="*50)
    print("✅ Exploration terminée !")


if __name__ == "__main__":
    explore_parquet_files()
