// ============================================================================
// CONFIGURATION: BIGQUERY & FOLDERS
// ============================================================================
const GCP_PROJECT_ID    = 'sit-ldl-int-oi-a-lvzt-run-818b'; 
const DATASET_ID        = 'imports_v4'; 
const ARCHIVE_FOLDER_ID = '1IOrUiTS_xXb69EBUcb8rqPSbUhQKjugO';

function getFolderConfigForBQLoader_() {
  if (typeof getPipelineFolderConfig === 'function') {
    try {
      return getPipelineFolderConfig();
    } catch (e) {
      console.warn('[BQ] Falling back to hardcoded folder IDs: ' + e.message);
    }
  }

  return {
    uploads: { id: '', name: '01_Uploads' },
    ready: { id: typeof READY_FOLDER_ID !== 'undefined' ? READY_FOLDER_ID : '', name: '02_Ready' },
    archive: { id: ARCHIVE_FOLDER_ID, name: '03_Archive' },
    output: { id: '', name: '04_Output' }
  };
}

// List of tracked files to set row logic
const FILE_RULES = [
  { keyword: "übersicht überschneiderartikel", headerRow: 2, dataRow: 3 }, 
  { keyword: "osnl",                           headerRow: 2, dataRow: 3 }, 
  { keyword: "osde",                           headerRow: 2, dataRow: 3 },
  { keyword: "osbe",                           headerRow: 2, dataRow: 3 },
  { keyword: "oscz",                           headerRow: 2, dataRow: 3 },
  { keyword: "oses",                           headerRow: 2, dataRow: 3 },
  { keyword: "osfr",                           headerRow: 2, dataRow: 3 },
  { keyword: "ospl",                           headerRow: 2, dataRow: 3 },
  { keyword: "ossk",                           headerRow: 2, dataRow: 3 },
  { keyword: "aktionsplan int_ltu",            headerRow: 1, dataRow: 2 }, 
  { keyword: "aktionsplan",                    headerRow: 1, dataRow: 2 }, 
  { keyword: "allocation",                     headerRow: 1, dataRow: 2 },
  { keyword: "ausnahmeliste",                  headerRow: 1, dataRow: 2 }, 
  { keyword: "baugleich import",               headerRow: 1, dataRow: 2 }, 
  { keyword: "bäf_de",                         headerRow: 7, dataRow: 9 }, 
  { keyword: "dearchiv",                       headerRow: 1, dataRow: 2 },
  { keyword: "export pt",                      headerRow: 1, dataRow: 2 }, 
  { keyword: "gesamt export cbx",              headerRow: 1, dataRow: 2 },
  { keyword: "lagerliste",                     headerRow: 1, dataRow: 2 },
  { keyword: "product ratings report",         headerRow: 1, dataRow: 2 }, 
  { keyword: "wt stationär",                   headerRow: 1, dataRow: 2 },
  { keyword: "db abfrage_t_dim_product_variant", headerRow: 1, dataRow: 2 },
  { keyword: "db abfrage nachbetrachtung",       headerRow: 1, dataRow: 2 },
  { keyword: "ganzjahresartikel",                headerRow: 1, dataRow: 2 },
  { keyword: "artikelkette",                     headerRow: 1, dataRow: 2 }
];

const OS_SHARED_SCHEMA = { kopfartikel: "STRING", ergebnis: "FLOAT64" };
const OSDE_SCHEMA = { kopfartikel: "STRING", summe_von_st_rwa: "FLOAT64", summe_von_rwa_anteil_land: "FLOAT64", summe_von_rwa_anteil_land_vormonat: "FLOAT64", summe_von_delta: "FLOAT64" };

const FILE_SPECIAL_OPTIONS = [
  { keyword: "osde", typeOverrides: OSDE_SCHEMA }, 
  { keyword: "osnl", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "osbe", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "oscz", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "oses", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "osfr", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "ospl", typeOverrides: OS_SHARED_SCHEMA },
  { keyword: "ossk", typeOverrides: OS_SHARED_SCHEMA },
  {
    keyword: "aktionsplan int_ltu",
    typeOverrides: {
      ian: "INT64", laenderspezifische_sap_nummern: "FLOAT64", bezeichnung: "STRING", charge: "INT64",
      technischer_status: "STRING", marke: "STRING", markentyp: "STRING", beduerfniswelten: "STRING",
      beduerfniskategorie: "STRING", laendervariante: "STRING", saisonauftakt: "FLOAT64", lt_split: "STRING",
      erster_lt: "STRING", erster_wt: "STRING", shop: "STRING", wt_id: "INT64", liefertermin: "STRING",
      werbetermin: "STRING", vk_datum: "STRING", kampagneninformation: "FLOAT64", kampagnen_startdatum: "STRING",
      kampagnen_enddatum: "STRING", werbeimpuls: "STRING", wdh: "FLOAT64", abverkaufshorizont: "INT64",
      tv_artikel: "FLOAT64", thema_nat: "STRING", abverkaufshorizont_nat: "FLOAT64", bestellmenge: "INT64",
      aktionsmenge: "INT64", nachlaufmenge: "INT64", ek_netto: "FLOAT64", vk_brutto: "FLOAT64",
      ek_netto_nat: "FLOAT64", vk_brutto_nat: "FLOAT64", mwst: "INT64", umsatz_ek_netto: "FLOAT64",
      umsatz_vk_brutto: "FLOAT64", umsatz_vk_netto: "FLOAT64", db_1: "FLOAT64", vk_kalk: "STRING",
      einkaeufer: "STRING", lieferant: "STRING", produktmanager_nat: "STRING", herkunft: "STRING",
      ian_vorgaenger: "FLOAT64", referenzartikel: "FLOAT64", artikeltyp: "STRING", sortimentstyp: "STRING",
      nfsourcingstrategie: "FLOAT64", mehrfachpack: "FLOAT64", saisonkennzeichen: "STRING", warengruppe: "FLOAT64",
      thema_nr: "FLOAT64", thema: "STRING", ruecknahmevereinbarung: "STRING", kollifaktor: "INT64",
      palettenfaktor: "INT64", abwicklungsart: "STRING", gefahrgut: "STRING", strecke: "STRING",
      versandfaehige_verpackung: "STRING", packstuecktyp: "STRING", versandart: "STRING",
      geodatenklassifizierung: "STRING", laenge: "FLOAT64", breite: "FLOAT64", hoehe: "FLOAT64",
      gewicht: "FLOAT64", verkaufsfaehig_fuer_de: "STRING", verkaufsfaehig_fuer_be: "STRING",
      verkaufsfaehig_fuer_nl: "STRING", verkaufsfaehig_fuer_cz: "STRING", verkaufsfaehig_fuer_es: "STRING",
      verkaufsfaehig_fuer_fr: "STRING", verkaufsfaehig_fuer_pl: "STRING", verkaufsfaehig_fuer_sk: "STRING",
      verkaufsfaehig_fuer_at: "STRING", verkaufsfaehig_fuer_hu: "STRING", verkaufsfaehig_fuer_dk: "STRING",
      verkaufsfaehig_fuer_it: "STRING", verkaufsfaehig_fuer_pt: "FLOAT64", verkaufsfaehig_fuer_se: "FLOAT64",
      verkaufsfaehig_fuer_fi: "FLOAT64", bezeichnung_nat: "STRING", kommentar_nat: "STRING", kommentar: "STRING"
    }
  },
  {
    keyword: "aktionsplan",
    typeOverrides: {
      ian: "STRING", laenderspezifische_sap_nummern: "INT64", bezeichnung: "STRING", charge: "INT64",
      technischer_status: "STRING", marke: "STRING", markentyp: "STRING", beduerfniswelten: "STRING",
      beduerfniskategorie: "STRING", laendervariante: "STRING", saisonauftakt: "STRING", lt_split: "STRING",
      erster_lt: "STRING", erster_wt: "STRING", shop: "STRING", wt_id: "INT64", liefertermin: "STRING",
      werbetermin: "STRING", vk_datum: "STRING", kampagneninformation: "STRING", kampagnen_startdatum: "STRING",
      kampagnen_enddatum: "STRING", werbeimpuls: "STRING", wdh: "STRING", abverkaufshorizont: "INT64",
      tv_artikel: "STRING", thema_nat: "STRING", abverkaufshorizont_nat: "INT64", bestellmenge: "INT64",
      aktionsmenge: "INT64", nachlaufmenge: "INT64", ek_netto: "FLOAT64", vk_brutto: "FLOAT64",
      ek_netto_nat: "FLOAT64", vk_brutto_nat: "FLOAT64", mwst: "INT64", umsatz_ek_netto: "FLOAT64",
      umsatz_vk_brutto: "FLOAT64", umsatz_vk_netto: "FLOAT64", db_1: "FLOAT64", vk_kalk: "STRING",
      einkaeufer: "STRING", lieferant: "STRING", produktmanager_nat: "STRING", herkunft: "STRING",
      ian_vorgaenger: "INT64", referenzartikel: "INT64", artikeltyp: "STRING", sortimentstyp: "STRING",
      nfsourcingstrategie: "STRING", mehrfachpack: "STRING", saisonkennzeichen: "STRING", warengruppe: "FLOAT64",
      thema_nr: "FLOAT64", thema: "STRING", ruecknahmevereinbarung: "STRING", kollifaktor: "INT64",
      palettenfaktor: "INT64", abwicklungsart: "STRING", gefahrgut: "STRING", strecke: "STRING",
      versandfaehige_verpackung: "STRING", packstuecktyp: "STRING", versandart: "STRING",
      geodatenklassifizierung: "STRING", laenge: "FLOAT64", breite: "FLOAT64", hoehe: "FLOAT64",
      gewicht: "FLOAT64", verkaufsfaehig_fuer_de: "STRING", verkaufsfaehig_fuer_be: "STRING",
      verkaufsfaehig_fuer_nl: "STRING", verkaufsfaehig_fuer_cz: "STRING", verkaufsfaehig_fuer_es: "STRING",
      verkaufsfaehig_fuer_fr: "STRING", verkaufsfaehig_fuer_pl: "STRING", verkaufsfaehig_fuer_sk: "STRING",
      verkaufsfaehig_fuer_at: "STRING", verkaufsfaehig_fuer_hu: "STRING", verkaufsfaehig_fuer_dk: "STRING",
      verkaufsfaehig_fuer_it: "STRING", verkaufsfaehig_fuer_pt: "STRING", verkaufsfaehig_fuer_se: "STRING",
      verkaufsfaehig_fuer_fi: "STRING", bezeichnung_nat: "STRING", kommentar_nat: "STRING", kommentar: "STRING"
    }
  },
  {
    keyword: "allocation",
    typeOverrides: {
      artikelnummer: "INT64", name: "STRING", virtuelles_warenhaus: "STRING",
      startdatum: "STRING", enddatum: "STRING", planwert: "INT64", reserve: "INT64",
      aufloeungsdatum: "FLOAT64", abverkauf: "INT64", abgedeckter_restwert_berechnet: "INT64",
      abgedeckter_restwert_effektiv: "INT64", sap_atp: "INT64", verkaufbarer_bestand: "INT64",
      sperrbestand: "INT64", globalbestand_lager: "INT64", globalbestand_zwischenlaeger: "INT64",
      globalbestand_echt: "INT64", echt_bestand_rod: "INT64", echt_bestand_rod2: "INT64",
      echt_bestand_afs: "INT64", echt_bestand_sap_atp: "INT64", globalbestand_venlo: "INT64",
      venlo_bestand_rod: "INT64", venlo_bestand_rod2: "INT64", venlo_bestand_afs: "INT64",
      venlo_bestand_sap_atp: "INT64", verfuegbarkeit: "STRING", wt_id: "STRING", status: "STRING"
    }
  },
  {
    keyword: "ausnahmeliste",
    renameColumns: { "col_": "kommentar" },
    typeOverrides: {
      kopfartikel: "INT64", kopfartikelbezeichnung: "STRING", artikeltyp: "STRING",
      urspruengliches_verwertungs_datum: "STRING", betroffene_laender: "STRING",
      genehmigt_ipm_kuerzel: "STRING", datum_genehmigung: "STRING", genehmigte_massnahme: "STRING",
      neuer_verwertungstermin_datum: "STRING", neuer_verwertungstermin_kw: "STRING",
      anfrage_durch_land_am: "STRING", anfragendes_land: "STRING", kommentar: "STRING"
    }
  },
  {
    keyword: "bäf_de",
    typeOverrides: {
      ocm: "STRING", ian: "INT64", sap_artikelnummer: "INT64", bezeichnung: "STRING",
      vk_datum: "DATE", end_datum: "DATE", werbe_impuls: "STRING", kampagnen_information: "STRING",
      startdatum: "DATE", enddatum: "DATE", tv: "STRING", aktions_menge: "INT64",
      vk_brutto: "FLOAT64", allokation_gewuenscht: "STRING", menge_on_top: "STRING",
      landesanmerkung_wdh: "STRING", thema_nat: "STRING", ek: "STRING", marge: "STRING",
      ian_im_amc: "STRING", sap_im_amc: "STRING", bereits_im_amc: "STRING", daten_vollstaendig: "STRING",
      meldung_fs_zu_spaet: "STRING", meldung_dd_zu_spaet: "STRING", wi_fs_oder_dd: "STRING",
      allokiert: "STRING", anzahl_varianten: "STRING"
    }
  },
  {
    keyword: "dearchiv",
    typeOverrides: {
      ocm: "STRING", organisationsebene: "STRING", sap_kopf: "INT64", 
      ian_abw_stationaer: "FLOAT64", ian: "STRING"
    }
  },
  {
    keyword: "baugleich import",
    renameColumns: { "col_": "second_column" },
    typeOverrides: {
       artikel: "STRING",
       second_column: "STRING" 
    }
  },
  {
    keyword: "export pt",
    typeOverrides: {
      zone: "STRING", article_number: "INT64", ian: "STRING", name: "STRING", ean: "STRING",
      msrp: "FLOAT64", previous_selling_price: "FLOAT64", current_selling_price: "FLOAT64",
      previous_shipping_surcharge: "FLOAT64", current_shipping_surcharge: "FLOAT64",
      itp: "FLOAT64", freightage: "FLOAT64", logistics_costs: "FLOAT64", global_status: "STRING",
      listing_status: "STRING", online_status: "STRING", supplier_number: "STRING", supplier: "STRING",
      logistics: "STRING", stock: "INT64", number_competitor_prices: "INT64", displayed_in_store: "STRING",
      item_family_code: "STRING", item_family_name: "STRING", brick_code: "INT64", created_by: "STRING"
    }
  },
  {
    keyword: "gesamt export cbx",
    typeOverrides: {
      ian: "INT64", ausm_nr: "STRING", artikelbezeichnung: "STRING", vorgaenger: "INT64",
      vorgaenger_2: "INT64", vorgaenger_3: "INT64", vorgaenger_4: "STRING", warengruppe: "STRING",
      ki: "INT64", referenzvorgaenger: "INT64", referenzartikel_eu_usa: "INT64", thema_am: "STRING",
      lt_am_land: "STRING", wt_am_land: "STRING", ekl: "STRING", gf: "STRING", marke: "STRING",
      zvp: "FLOAT64", gesamtmenge: "INT64", bestellmenge_de: "FLOAT64", bestellmenge_osde: "FLOAT64",
      bestellbar_fuer: "STRING", bewertungen_uebernehmen_os_code: "STRING", bewertungen_uebernehmen_os: "STRING"
    }
  },
  {
    keyword: "lagerliste",
    typeOverrides: {
      stichtag: "STRING", wshop_cd: "STRING", ocm: "STRING", beduerfniswelten: "STRING",
      organisationsebene: "STRING", artikelfamilie: "FLOAT64", sap_kopf: "INT64", ian: "FLOAT64",
      bezeichnung: "STRING", bundle_set: "STRING", eigenmarke_marke: "STRING", sortimentsklasse: "STRING",
      dauerdispo: "STRING", altersstruktur_stand_ende_vorvormonat: "STRING",
      altersstruktur_stand_ende_vormonat: "STRING", aenderung_altersstruktur: "STRING", marke: "STRING",
      bestand_webshop_ende_vormonat: "FLOAT64", ek_volumen_webshop_ende_vormonat: "FLOAT64",
      bestand_int_ende_vormonat: "FLOAT64", ek_volumen_int_ende_vormonat: "FLOAT64",
      bestand_land_aktuell: "FLOAT64", ek_volumen_land: "FLOAT64", durchschn_wochenabverkauf_8_wochen: "FLOAT64",
      reichweite_wochen_rechn_restbestand: "FLOAT64", reichweite_monate_rechn_restbestand: "FLOAT64",
      venlo_bestand: "FLOAT64", variantenverfuegbarkeit_shop_land: "STRING",
      variantenverfuegbarkeit_alle_laender: "STRING", bestand_alle_laender: "INT64",
      durchschn_letzter_einkaufspreis_netto: "FLOAT64", retourenquote_aufg_3_gj: "FLOAT64",
      online_offline: "STRING", grossteil: "STRING", lt: "STRING", verwertungsdatum: "STRING",
      artikeltyp: "STRING", global_offline_verwertung_status33: "STRING", global_status_code: "INT64",
      prod_online_status_cd: "FLOAT64", prod_listing_status_cd: "INT64", physischer_bestand_blo: "INT64",
      physischer_bestand_hue: "INT64", physischer_bestand_lud: "INT64", physischer_bestand_pil: "INT64",
      physischer_bestand_rot: "INT64", physischer_bestand_venlo: "INT64", physischer_bestand_roosendal: "INT64",
      physischer_bestand_sosnowiecz: "INT64", physischer_bestand_ech: "INT64", physischer_bestand_ses: "INT64",
      physischer_bestand_pin: "INT64"
    }
  },
  {
    keyword: "product ratings report",
    typeOverrides: {
      head_number: "INT64",
      product_name: "STRING",
      ratings: "FLOAT64", 
      stars: "STRING"    
    }
  },
  {
    keyword: "übersicht überschneiderartikel",
    typeOverrides: {
      ian: "INT64", artikelbez: "STRING", vertriebskanaele: "STRING", wer_definiert_mindest_vk: "STRING",
      ek: "FLOAT64", zvp: "FLOAT64", fruehester_lt: "STRING", listungsart_kl: "STRING", listungsart_lidl: "STRING",
      werbewoche_kl: "STRING", aktion_vk_kl: "FLOAT64", sortiment_vk_kl: "FLOAT64",
      wettbewerber_inklusive_preis: "STRING", gtin_kl: "STRING", werbewoche_lidl: "STRING",
      aktion_vk_lidl: "FLOAT64", sortiment_vk_lidl: "FLOAT64", wettbewerber_lidl: "STRING",
      wettbewerber_preis: "FLOAT64", versandkosten_lidl_os: "FLOAT64", vorgaenger_wt: "STRING",
      v1_ian: "STRING", v2_ian: "STRING", v3_ian: "STRING", aktions_vk: "FLOAT64",
      aktions_vk_inkl_versandkosten: "FLOAT64", sortiment_vk: "FLOAT64",
      sortiment_vk_inkl_versandkosten: "FLOAT64", kommentar_ocm_pricing_zur_abstimmung_mit_kl: "STRING",
      abgestimmt_kw: "STRING", sap_nummer: "STRING"
    }
  },
  {
    keyword: "wt stationär",
    typeOverrides: {
      art_nr: "INT64", art_bezeichnung_nat: "STRING", art_bezeichnung_de: "STRING", chargen_nr: "STRING",
      bemerkung_4: "STRING", marke: "STRING", m_em: "STRING", lt: "STRING", wt: "STRING", gueltig_ab: "STRING",
      regionen: "STRING", ek_krz_int: "STRING", ek_name_nat: "STRING", werbemittel_nat: "STRING",
      werbemittel_int: "STRING", beduerfniswelt: "STRING", aktionsunterbereich: "STRING", uwg: "STRING",
      af: "STRING", thema_nr: "STRING", thema: "STRING", vorgaenger_art_nr: "STRING", lieferant: "STRING",
      mwst: "FLOAT64", menge: "FLOAT64", ki: "STRING", stk_fil: "FLOAT64", ek_nto: "FLOAT64", vk_neu: "FLOAT64",
      ek_volumen: "FLOAT64", umsatz_op_plan_neu: "FLOAT64", rohertrag_op_plan_neu: "FLOAT64",
      aktions_kalk_op_plan_neu: "FLOAT64", bemerkung_1: "STRING", kontrakt: "STRING", uvp: "FLOAT64",
      kl_artikel: "STRING", zvp: "FLOAT64", abwicklungsart_de: "STRING", anlief_ges_ab: "STRING", pf: "STRING",
      theor_pf: "STRING", kampagne: "STRING", bemerkung_land: "STRING", wz: "STRING", display: "STRING",
      status: "STRING", art_nr_wawi: "STRING", freigabe_wawi: "STRING", bemerkung_2: "STRING", ve_wunsch: "STRING",
      bemerkung_an_ekl: "STRING", aufgeteilte_lieferung: "STRING", bemerkung_5: "STRING",
      filialplatzierung_int: "STRING", artikeltyp: "STRING"
    }
  },
  {
    keyword: "db abfrage_t_dim_product_variant",
    typeOverrides: {
      prod_nr: "INT64",
      prod_size_type_cd: "STRING",
      palette_capacity_cnt: "INT64"
    }
  },
  {
    keyword: "db abfrage nachbetrachtung", 
    typeOverrides: {
      laenderspezifische_sap_nummern: "INT64",
      shop: "STRING",
      promo_nr: "FLOAT64",
      promo_descr: "STRING",
      saisonkennzeichen: "STRING",
      marke: "STRING",
      markentyp: "STRING",
      artikeltyp: "STRING",
      world_of_need_cd: "INT64",
      world_of_need_name: "STRING",
      category_of_need_cd: "INT64",
      category_of_need_name: "STRING",
      prod_family_cd: "FLOAT64",
      mwst: "INT64",
      max_liefertermin_datum: "STRING",
      abverkaufshorizont_angepasst: "INT64",
      max_liefertermin_kw: "STRING",
      ian: "STRING",
      abverkaufshorizont: "FLOAT64",
      bisherige_bestellmenge: "INT64",
      zukuenftige_bestellmenge: "INT64",
      vk_aktuell: "FLOAT64",
      vk_alt: "FLOAT64",
      verwertungszeitpunkt: "STRING",
      verwertungszeitpunkt_kw_jahr: "STRING",
      jetzt_verwerten: "STRING",
      col_1_betrachtungszeitpunkt: "STRING",
      col_1_betrachtungszeitpunkt_kw_jahr: "STRING",
      col_2_betrachtungszeitpunkt: "STRING",
      col_2_betrachtungszeitpunkt_kw_jahr: "STRING",
      col_3_betrachtungszeitpunkt: "STRING",
      col_3_betrachtungszeitpunkt_kw_jahr: "STRING",
      col_4_betrachtungszeitpunkt: "STRING",
      col_4_betrachtungszeitpunkt_kw_jahr: "STRING",
      col_5_betrachtungszeitpunkt: "STRING",
      col_5_betrachtungszeitpunkt_kw_jahr: "STRING",
      betrachtungszeitpunkte: "STRING",
      zu_betrachten: "STRING"
    }
  },
  {
    keyword: "ganzjahresartikel",
    keepColumnIndexes: [0, 1],
    typeOverrides: {
      ian: "STRING",
      ganzjahres: "STRING"
    }
  },
  {
    keyword: "artikelkette",
    keepColumnIndexes: [0, 1],
    typeOverrides: {
      ian: "STRING",
      artikelkette: "STRING"
    }
  }
];

// ============================================================================
// GLOBAL ROBUST RETRY WRAPPER
// Resolves Transient BigQuery and DriveApp 'Service error' exceptions
// ============================================================================
function executeWithRetry_(action, maxRetries = 3) {
  for (let attempts = 1; attempts <= maxRetries; attempts++) {
    try {
      return action();
    } catch (e) {
      const msg = e.message || String(e);
      if (attempts < maxRetries) {
        console.warn(`[RETRY] Error caught: ${msg}. Retrying ${attempts}/${maxRetries} in 3 seconds...`);
        Utilities.sleep(3000 * attempts); // Backoff progresiv
      } else {
        throw e;
      }
    }
  }
}

// ============================================================================
// HELPERS (DATA TYPE DETECTION & FORMATTING)
// ============================================================================
function normalizeNumberish_(value, fileDelimiter) {
  let v = String(value || '').trim();
  if (!v) return '';
  v = v.replace(/[€$£\s]/g, '').replace(/[^0-9,.-]/g, '');
  if (!v) return '';
  return fileDelimiter === ';' ? v.replace(/\./g, '').replace(/,/g, '.') : v.replace(/,/g, '');
}

function isDateLikeValue_(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return false;
  if (/^\d{4}[-\/.]\d{1,2}[-\/.]\d{1,2}(?:[ T].*)?$/.test(raw)) return true;
  if (/^\d{1,2}[-\/.]\d{1,2}[-\/.]\d{2,4}(?:[ T].*)?$/.test(raw)) return true;
  return false;
}

function inferDataTypeFromSamples_(samples, fileDelimiter) {
  const values = (samples || [])
    .map(v => String(v == null ? '' : v).trim())
    .filter(v => v && !/^null$/i.test(v) && v !== '-');

  if (!values.length) return 'STRING';

  let [dateCount, boolCount, intCount, decimalCount, intOverflowCount] = [0, 0, 0, 0, 0];
  const INT64_MIN_BI = BigInt('-9223372036854775808');
  const INT64_MAX_BI = BigInt('9223372036854775807');

  for (let raw of values) {
    if (/^(true|false|yes|no|ja|nein|0|1)$/i.test(raw)) { boolCount++; continue; }
    if (isDateLikeValue_(raw)) { dateCount++; continue; }

    const normalized = normalizeNumberish_(raw, fileDelimiter);
    if (!normalized || Number.isNaN(Number(normalized))) continue;

    if (/^-?\d+$/.test(normalized)) {
      intCount++;
      const bi = BigInt(normalized);
      if (bi < INT64_MIN_BI || bi > INT64_MAX_BI) intOverflowCount++;
    } else {
      decimalCount++;
    }
  }

  const total = values.length;
  if (dateCount / total >= 0.85) return 'DATE';
  if (boolCount / total >= 0.9) return 'BOOL';
  if ((intCount + decimalCount) / total >= 0.85) {
    if (intCount > 0 && decimalCount === 0 && intOverflowCount > 0) return 'STRING';
    return decimalCount > 0 ? 'NUMERIC' : 'INT64';
  }
  return 'STRING';
}

function resolveColumnTypeWithOverrides_(headerName, sampleValues, fileDelimiter, typeOverrides) {
  const key = String(headerName || '').toLowerCase();
  
  if (typeOverrides && typeOverrides[key]) {
    return typeOverrides[key];
  }
  
  if (typeOverrides) {
    console.warn(`⚠️ [SCHEMA WARN] Column "${key}" not found in predefined schema`);
  }
  
  return inferDataTypeFromSamples_(sampleValues, fileDelimiter);
}

function getSpecialFileOptions_(lowerName) {
  return FILE_SPECIAL_OPTIONS.find(opt => lowerName.includes(opt.keyword)) || null;
}

function columnIndexToA1_(columnIndex) {
  let index = Number(columnIndex);
  if (!Number.isFinite(index) || index < 0) return null;
  let result = '';
  while (index >= 0) {
    result = String.fromCharCode((index % 26) + 65) + result;
    index = Math.floor(index / 26) - 1;
  }
  return result;
}

function buildSheetRangeFromColumnIndexes_(indexes) {
  if (!indexes || !indexes.length) return null;
  let normalized = indexes.map(Number).filter(n => Number.isInteger(n) && n >= 0).sort((a, b) => a - b);
  if (!normalized.length) return null;

  for (let i = 1; i < normalized.length; i++) {
    if (normalized[i] !== normalized[i - 1] + 1) return null;
  }

  let start = columnIndexToA1_(normalized[0]);
  let end = columnIndexToA1_(normalized[normalized.length - 1]);
  return start && end ? `${start}:${end}` : null;
}

function buildRawHeaders_(parsedRows, lineRows, fileDelimiter, headerRow, headerRowByColumn, keepColumnIndexes) {
  let baseHeaders = (parsedRows && parsedRows.length) 
    ? (parsedRows[headerRow - 1] || []) 
    : String(lineRows[headerRow - 1] || '').split(fileDelimiter);

  let headers = [...baseHeaders];

  if (headerRowByColumn) {
    Object.keys(headerRowByColumn).forEach(k => {
      let [colIndex, rowNumber] = [Number(k), Number(headerRowByColumn[k])];
      if (Number.isInteger(colIndex) && Number.isInteger(rowNumber) && rowNumber > 0) {
        let overrideRow = (parsedRows && parsedRows.length) 
          ? (parsedRows[rowNumber - 1] || []) 
          : String(lineRows[rowNumber - 1] || '').split(fileDelimiter);
        headers[colIndex] = overrideRow[colIndex] || '';
      }
    });
  }

  if (keepColumnIndexes && keepColumnIndexes.length) {
    headers = keepColumnIndexes.map(idx => headers[idx] != null ? headers[idx] : '');
  }
  return headers;
}

// ============================================================================
// MASSIVE FILE AUTO-CHUNKER & INDEX INJECTOR (With Fallback)
// ============================================================================
function autoChunkAndIndexMassiveCsv_(fileId, fileName, fileDelimiter, headerRow, dataRow) {
  const token = ScriptApp.getOAuthToken();
  const fileUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;
  
  const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=size`;
  const metaRes = UrlFetchApp.fetch(metaUrl, { headers: { 'Authorization': 'Bearer ' + token } });
  const fileSize = parseInt(JSON.parse(metaRes.getContentText()).size, 10);
  
  const CHUNK_SIZE = 15 * 1024 * 1024;
  let startByte = 0;
  let leftoverText = ""; 
  let globalRowIndex = 1;
  let chunkNumber = 1;
  
  let tempFilesToDelete = [];
  let tempFileUris = [];
  
  console.log(`[AUTO-CHUNK] File of ${(fileSize / 1024 / 1024).toFixed(2)} MB detected. Starting byte-stream slicing...`);

  while (startByte < fileSize) {
    let endByte = Math.min(startByte + CHUNK_SIZE - 1, fileSize - 1);
    
    let res;
    try {
      res = UrlFetchApp.fetch(fileUrl, {
        headers: { 'Authorization': 'Bearer ' + token, 'Range': `bytes=${startByte}-${endByte}` },
        muteHttpExceptions: false
      });
    } catch (e) {
      console.warn(`[AUTO-CHUNK] URLFetch Quota / Transfer Limit hit: ${e.message}`);
      throw new Error("URLFETCH_QUOTA_EXCEEDED"); // Trigger fallback
    }
    
    let chunkText = leftoverText + res.getContentText();
    
    if (endByte < fileSize - 1) {
      let lastNewlineIdx = chunkText.lastIndexOf('\n');
      if (lastNewlineIdx !== -1) {
        leftoverText = chunkText.substring(lastNewlineIdx + 1); 
        chunkText = chunkText.substring(0, lastNewlineIdx); 
      }
    } else {
      leftoverText = "";
    }
    
    let csvData;
    try {
      csvData = Utilities.parseCsv(chunkText, fileDelimiter);
    } catch(e) {
      csvData = chunkText.split('\n').map(row => row.split(fileDelimiter));
    }
    
    for (let i = 0; i < csvData.length; i++) {
      if (chunkNumber === 1 && i === headerRow - 1) {
        csvData[i].unshift('index'); 
      } else if (chunkNumber === 1 && i < dataRow - 1) {
        csvData[i].unshift(''); 
      } else {
        csvData[i].unshift(globalRowIndex++); 
      }
    }
    
    const newCsvText = csvData.map(row => row.map(cell => {
      let str = String(cell || '');
      if (str.includes(fileDelimiter) || str.includes('"') || str.includes('\n')) {
        return '"' + str.replace(/"/g, '""') + '"';
      }
      return str;
    }).join(fileDelimiter)).join('\n');
    
    // Wrapped in retry to prevent "Service Error: Drive"
    let tempFile = executeWithRetry_(() => DriveApp.createFile(`temp_idx_chunk_${chunkNumber}_${fileName}`, newCsvText, MimeType.CSV));
    tempFilesToDelete.push(tempFile);
    tempFileUris.push(`https://drive.google.com/open?id=${tempFile.getId()}`);
    
    console.log(`[AUTO-CHUNK] Chunk ${chunkNumber} completed. Index reached ${globalRowIndex - 1}.`);
    
    startByte = endByte + 1;
    chunkNumber++;
  }
  
  return { tempFileUris: tempFileUris, tempFilesObjects: tempFilesToDelete };
}

// ============================================================================
// UI TRIGGER & UTILITIES 
// ============================================================================
function openBQProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('BQProgressUI')
    .setWidth(600).setHeight(500).setTitle('BigQuery Ingestion Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Database Loader');
}

function cleanTableName(fileName) {
  let raw = fileName.replace(/\.csv$/i, '').replace(/\.xlsx?$/i, '').replace(/\.xlsb$/i, '');
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  let en = raw.replace(/[äöüÄÖÜß]/g, m => map[m]);
  return 'raw_' + en.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
}

function getReadyFiles() {
  console.log("[INIT] Scanning '02_Ready' folder for files to import...");
  const folderCfg = getFolderConfigForBQLoader_();
  
  // Retry fetch folder and files to avoid Drive errors
  const folder = executeWithRetry_(() => DriveApp.getFolderById(folderCfg.ready.id));
  const files = executeWithRetry_(() => folder.getFiles());
  
  let fileMap = {};
  let standaloneFiles = [];

  while (executeWithRetry_(() => files.hasNext())) {
    let f = executeWithRetry_(() => files.next());
    let name = executeWithRetry_(() => f.getName());
    
    let chunkMatch = name.match(/^(.*)__chunk_(\d+)\.csv$/i);
    if (chunkMatch) {
      let baseName = chunkMatch[1];
      let chunkIndex = parseInt(chunkMatch[2], 10);
      
      if (!fileMap[baseName]) {
        fileMap[baseName] = { id: baseName, name: baseName + '.csv', parts: [] };
      }
      fileMap[baseName].parts.push({ id: f.getId(), name: name, index: chunkIndex });
    } else {
      standaloneFiles.push({ id: f.getId(), name: name, mimeType: f.getMimeType() });
    }
  }
  
  let chunkedFiles = Object.values(fileMap).map(grp => {
    grp.parts.sort((a, b) => a.index - b.index);
    return grp;
  });

  let finalList = standaloneFiles.concat(chunkedFiles);
  console.log(`[INIT] Found ${finalList.length} datasets ready for import.`);
  return finalList;
}

// ============================================================================
// SCHEMA DETECTOR (With Fallback)
// ============================================================================
function buildDynamicSchema(fileId, headerRow, dataRow, forcedDelimiter, projectId, datasetId, mimeType, specialOptions) {
  console.log(`[SCHEMA] Phase 1: Fetching file ID ${fileId}...`);
  let url = mimeType === MimeType.GOOGLE_SHEETS 
    ? `https://docs.google.com/spreadsheets/d/${fileId}/export?format=csv` 
    : `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;

  let rawText = "";
  try {
    let response = UrlFetchApp.fetch(url, {
      headers: { 'Authorization': 'Bearer ' + ScriptApp.getOAuthToken(), 'Range': 'bytes=0-500000' }
    });
    rawText = response.getContentText();
  } catch (e) {
    console.warn(`[SCHEMA] UrlFetchApp Limit Reached. Fallback to DriveApp...`);
    // Retry Blob fetching for Google Drive fallback
    let blob = executeWithRetry_(() => DriveApp.getFileById(fileId).getBlob());
    rawText = blob.getDataAsString().substring(0, 500000);
  }
  
  console.log(`[SCHEMA] Phase 2: Parsing CSV bytes...`);
  let lines = rawText.split(/\r?\n/);
  
  const opts = specialOptions || {};
  let fileDelimiter = forcedDelimiter || ((lines[0] || "").match(/;/g)?.length > (lines[0] || "").match(/,/g)?.length ? ';' : ',');
  
  let rawHeaders = [], sampleRows = [];
  try { 
    let parsed = Utilities.parseCsv(rawText, fileDelimiter); 
    rawHeaders = buildRawHeaders_(parsed, lines, fileDelimiter, headerRow, opts.headerRowByColumn, opts.keepColumnIndexes);
    sampleRows = parsed.slice(dataRow - 1, Math.min(parsed.length, dataRow - 1 + 40));
    if (opts.keepColumnIndexes) sampleRows = sampleRows.map(r => opts.keepColumnIndexes.map(idx => r && r[idx] != null ? r[idx] : ''));
  } catch(e) { 
    rawHeaders = buildRawHeaders_(null, lines, fileDelimiter, headerRow, opts.headerRowByColumn, opts.keepColumnIndexes);
    let fallback = (lines[dataRow - 1] || "").split(fileDelimiter);
    if (opts.keepColumnIndexes) fallback = opts.keepColumnIndexes.map(idx => fallback[idx] != null ? fallback[idx] : '');
    sampleRows = [fallback];
  }

  console.log(`[SCHEMA] Phase 3-4: Translating and Deduplicating headers...`);
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  
  let englishHeaders = rawHeaders.map(val => {
    let en = String(val).replace(/[äöüÄÖÜß]/g, m => map[m]).replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase().replace(/_+/g, '_').replace(/^_|_$/g, '');
    
    if (!en) return 'col_empty_' + Math.floor(Math.random()*1000); 

    if (/^[0-9]/.test(en)) {
      return opts.allowNumberStart ? en.substring(0, 290) : 'col_' + en.substring(0, 290);
    }
    
    return en.substring(0, 290);
  });

  let used = new Set(), finalSchemaFields = [];
  for(let i = 0; i < englishHeaders.length; i++) {
    let f = englishHeaders[i];
    if (opts.renameColumns && opts.renameColumns[f]) f = opts.renameColumns[f];
    
    let c = 1;
    let baseName = f;
    while(used.has(f) && c < 500) { f = baseName + '_' + c++; }
    used.add(f);
    
    let sampleValues = sampleRows.map(r => r && r[i] != null ? r[i] : '');
    let detectedType = resolveColumnTypeWithOverrides_(f, sampleValues, fileDelimiter, opts.typeOverrides);
    
    finalSchemaFields.push({ name: f, type: detectedType });
  }
  
  return { schema: finalSchemaFields, delimiter: fileDelimiter };
}

// ============================================================================
// MAIN PIPELINE: PROCESS SINGLE FILE
// ============================================================================
function processSingleBQFile(fileObj) {
  const folderCfg = getFolderConfigForBQLoader_();
  
  // Wrap fetching objects in retry
  const archiveFolder = executeWithRetry_(() => DriveApp.getFolderById(folderCfg.archive.id));
  const file = executeWithRetry_(() => DriveApp.getFileById(fileObj.id));
  const lowerName = fileObj.name.toLowerCase();
  
  console.log(`\n======================================================\n[SERVER] STARTING IMPORT PIPELINE: ${fileObj.name}\n======================================================`);
  
  let headerRow = 1, dataRow = 2, forcedDelimiter = null; 
  let specialOptions = getSpecialFileOptions_(lowerName) || {};
  
  let ruleMatch = FILE_RULES.find(r => lowerName.includes(r.keyword));
  if (ruleMatch) {
    headerRow = ruleMatch.headerRow; dataRow = ruleMatch.dataRow; forcedDelimiter = ruleMatch.delimiter || null;
  }
  if (specialOptions.headerRow) headerRow = specialOptions.headerRow;
  if (specialOptions.dataRow) dataRow = specialOptions.dataRow;

  let tableName = cleanTableName(fileObj.name);
  let tempTableId = tableName + '_temp_ext'; 
  
  let tempFilesToDelete = []; 
  
  try {
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch (e) { }

    let { schema: finalSchema, delimiter: fileDelimiter } = buildDynamicSchema(fileObj.id, headerRow, dataRow, forcedDelimiter, GCP_PROJECT_ID, DATASET_ID, fileObj.mimeType, specialOptions);

    let isSheet = fileObj.mimeType === MimeType.GOOGLE_SHEETS;
    let externalUris = [];
    let chunkData = null;

    // AUTO-CHUNKER & INDEX INJECTION
    if (!isSheet && fileDelimiter) {
      try {
        console.log(`[PRE-PROCESS] Attempting to inject 'index' column into CSV...`);
        chunkData = autoChunkAndIndexMassiveCsv_(fileObj.id, fileObj.name, fileDelimiter, headerRow, dataRow);
        if (chunkData && chunkData.tempFileUris.length > 0) {
          finalSchema.unshift({ name: 'index', type: 'INT64' });
        }
      } catch (e) {
        console.warn(`[PRE-PROCESS] Failed to chunk (Quota Limit / Error). Fallback to DIRECT Import: ${e.message}`);
      }
    }

    let isChunked = (chunkData && chunkData.tempFileUris.length > 0);

    let externalDataConfiguration = {
      sourceFormat: isSheet ? "GOOGLE_SHEETS" : "CSV",
      autodetect: false,
      ignoreUnknownValues: true
    };

    if (isChunked) {
      externalUris = chunkData.tempFileUris;
      tempFilesToDelete = chunkData.tempFilesObjects;
      externalDataConfiguration.sourceUris = externalUris;
      externalDataConfiguration.csvOptions = { 
        skipLeadingRows: 0, 
        allowQuotedNewlines: true, 
        fieldDelimiter: fileDelimiter,
        allowJaggedRows: true 
      };
    } else {
      // NATIVE DRIVE FALLBACK
      externalUris = [`https://drive.google.com/open?id=${fileObj.id}`];
      externalDataConfiguration.sourceUris = externalUris;
      
      // Safety check: ensure index is removed if chunking failed
      finalSchema = finalSchema.filter(f => f.name !== 'index');

      if (isSheet) {
        externalDataConfiguration.googleSheetsOptions = { skipLeadingRows: dataRow - 1 };
        let limitedRange = buildSheetRangeFromColumnIndexes_(specialOptions.keepColumnIndexes || []);
        if (limitedRange) externalDataConfiguration.googleSheetsOptions.range = limitedRange;
      } else {
        externalDataConfiguration.csvOptions = { 
          skipLeadingRows: dataRow - 1, 
          allowQuotedNewlines: true, 
          fieldDelimiter: fileDelimiter,
          allowJaggedRows: true 
        };
      }
    }

    // Insert Temp Table using Robust Retry
    executeWithRetry_(() => {
      BigQuery.Tables.insert({
        tableReference: { projectId: GCP_PROJECT_ID, datasetId: DATASET_ID, tableId: tempTableId },
        schema: { fields: finalSchema.map(f => ({ name: f.name, type: 'STRING' })) }, 
        externalDataConfiguration
      }, GCP_PROJECT_ID, DATASET_ID);
    });

    let selectCols = finalSchema.map(f => {
      let colName = `\`${f.name}\``;
      let cleanStr = `CASE WHEN LOWER(TRIM(${colName})) IN ('', 'null', '-') OR STARTS_WITH(TRIM(${colName}), '#') THEN NULL ELSE TRIM(${colName}) END`;
      let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
      let isEuFormat = fileDelimiter === ';';

      switch (f.type.toUpperCase()) {
        case 'BIGNUMERIC': case 'NUMERIC': case 'FLOAT64':
          let replaceStr = isEuFormat ? `REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '.')` : `REPLACE(${noCurrency}, ',', '')`;
          return `SAFE_CAST(${replaceStr} AS ${f.type.toUpperCase()}) AS ${colName}`;
        case 'INT64':
          return isEuFormat ? `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '') AS INT64) AS ${colName}` : `SAFE_CAST(SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS NUMERIC) AS INT64) AS ${colName}`;
        case 'DATE':
          return `
            COALESCE(
              SAFE.PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(${cleanStr}, r'[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}')),
              SAFE.PARSE_DATE('%d.%m.%Y', REGEXP_EXTRACT(${cleanStr}, r'[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{4}')),
              SAFE.PARSE_DATE('%Y/%m/%d', REGEXP_EXTRACT(${cleanStr}, r'[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}')),
              SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(${cleanStr}, r'[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')),
              SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_EXTRACT(${cleanStr}, r'[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')),
              
              SAFE_CAST(SUBSTR(${cleanStr}, 1, 10) AS DATE),
              
              CASE 
                WHEN REGEXP_CONTAINS(${cleanStr}, r'^[0-9]{4,5}([,.][0-9]+)?$') 
                THEN DATE_ADD(DATE '1899-12-30', INTERVAL SAFE_CAST(REGEXP_EXTRACT(${cleanStr}, r'^([0-9]{4,5})') AS INT64) DAY)
                ELSE NULL 
              END
            ) AS ${colName}`.trim();
        case 'BOOL':
          return `CASE WHEN LOWER(TRIM(${colName})) IN ('true', '1', 'yes', 'ja') THEN TRUE WHEN LOWER(TRIM(${colName})) IN ('false', '0', 'no', 'nein') THEN FALSE ELSE NULL END AS ${colName}`;
        default:
          return `${cleanStr} AS ${colName}`;
      }
    }).join(',\n      ');

    let schemaForAnchor = finalSchema.filter(f => f.name !== 'index');
    let anchorFields = schemaForAnchor.slice(0, 3).map(f => `\`${f.name}\` IS NOT NULL AND LOWER(TRIM(CAST(\`${f.name}\` AS STRING))) != LOWER('${f.name}')`); 
    let anchorCondition = anchorFields.length > 0 ? anchorFields.join(' OR ') : '1=1';

    let hasIndex = finalSchema.some(f => f.name === 'index');
    let innerWhere = hasIndex ? "WHERE SAFE_CAST(\`index\` AS INT64) IS NOT NULL" : "";
    let orderByClause = hasIndex ? "ORDER BY \`index\` ASC" : "";

    let query = `
      CREATE OR REPLACE TABLE \`${GCP_PROJECT_ID}.${DATASET_ID}.${tableName}\` AS
      SELECT * FROM (
        SELECT ${selectCols} FROM \`${GCP_PROJECT_ID}.${DATASET_ID}.${tempTableId}\`
        ${innerWhere}
      )
      WHERE ${anchorCondition}
      ${orderByClause};
    `;

    // Insert Job using Robust Retry
    let insertedJob = executeWithRetry_(() => {
      return BigQuery.Jobs.insert({ configuration: { query: { query: query, useLegacySql: false } } }, GCP_PROJECT_ID);
    });

    let [maxAttempts, success, errorMsg] = [300, false, ""];
    for (let i = 0; i < maxAttempts; i++) {
      try {
        let job = BigQuery.Jobs.get(GCP_PROJECT_ID, insertedJob.jobReference.jobId, { location: insertedJob.jobReference.location });
        if (job.status.state === 'DONE') {
          if (job.status.errorResult) errorMsg = job.status.errorResult.message;
          else success = true;
          break;
        }
      } catch (e) { }
      Utilities.sleep(3000);
    }

    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e) {}
    
    // Trash temp chunks using retry to avoid API fail
    tempFilesToDelete.forEach(f => { executeWithRetry_(() => f.setTrashed(true)); });
    
    if (success) {
      executeWithRetry_(() => file.moveTo(archiveFolder)); 
      return { success: true, log: `[SUCCESS] Injected into '${tableName}'${hasIndex ? ' ordered by index' : ' (Direct Mode)'}. Moved to Archive.` };
    } 
    return { success: false, log: errorMsg ? `[ERROR] BigQuery rejected: ${errorMsg}` : `[ERROR] Timeout.` };
    
  } catch (error) {
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e){}
    tempFilesToDelete.forEach(f => { try { f.setTrashed(true); } catch(e){} });
    return { success: false, log: `[CRITICAL] Connection failed: ${error.message}` };
  }
}