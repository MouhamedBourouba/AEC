# RPA 99 / 2003 Seismic Zones mapping for Algerian Wilayas
# 0: Négligeable, I: Faible, IIa: Modérée, IIb: Assez élevée, III: Élevée

RPA_ZONES = {
    "01": "0",    # Adrar
    "02": "III",  # Chlef
    "03": "I",    # Laghouat
    "04": "IIa",  # Oum El Bouaghi
    "05": "IIa",  # Batna
    "06": "IIb",  # Bejaia
    "07": "I",    # Biskra
    "08": "I",    # Bechar
    "09": "III",  # Blida
    "10": "IIb",  # Bouira
    "11": "0",    # Tamanrasset
    "12": "IIa",  # Tebessa
    "13": "IIa",  # Tlemcen
    "14": "IIa",  # Tiaret
    "15": "IIb",  # Tizi Ouzou
    "16": "III",  # Alger
    "17": "I",    # Djelfa
    "18": "IIb",  # Jijel
    "19": "IIb",  # Setif
    "20": "IIa",  # Saida
    "21": "IIb",  # Skikda
    "22": "IIa",  # Sidi Bel Abbes
    "23": "IIb",  # Annaba
    "24": "IIb",  # Guelma
    "25": "IIb",  # Constantine
    "26": "IIb",  # Medea
    "27": "IIa",  # Mostaganem
    "28": "I",    # M'Sila
    "29": "IIa",  # Mascara
    "30": "0",    # Ouargla
    "31": "IIa",  # Oran
    "32": "I",    # El Bayadh
    "33": "0",    # Illizi
    "34": "IIb",  # Bordj Bou Arreridj
    "35": "III",  # Boumerdes
    "36": "IIb",  # El Tarf
    "37": "0",    # Tindouf
    "38": "IIa",  # Tissemsilt
    "39": "I",    # El Oued
    "40": "IIa",  # Khenchela
    "41": "IIa",  # Souk Ahras
    "42": "III",  # Tipaza
    "43": "IIb",  # Mila
    "44": "III",  # Ain Defla
    "45": "I",    # Naama
    "46": "IIa",  # Ain Temouchent
    "47": "I",    # Ghardaia
    "48": "IIa",  # Relizane
    "49": "0",    # Timimoun
    "50": "0",    # Bordj Badji Mokhtar
    "51": "I",    # Ouled Djellal
    "52": "0",    # Beni Abbes
    "53": "0",    # In Salah
    "54": "0",    # In Guezzam
    "55": "0",    # Touggourt
    "56": "0",    # Djanet
    "57": "I",    # El M'Ghair
    "58": "0"     # El Meniaa
}

def get_zone_for_wilaya(wilaya_str):
    if not wilaya_str:
        return "Inconnue"
    
    # Extract the code from a string like "16 - ALGER"
    import re
    m = re.match(r"^(\d{1,2})", wilaya_str.strip())
    if m:
        code = m.group(1).zfill(2)
        return RPA_ZONES.get(code, "Inconnue")
    
    return "Inconnue"
