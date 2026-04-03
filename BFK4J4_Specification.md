# Házi Feladat Specifikáció

**Data Engineering** - Opcionális házi feladat

---

## 1. Hallgató adatai

|                |                        |
| -------------- | ---------------------- |
| **Név**        | Horváth Gellért        |
| **Neptun-kód** | BFK4J4                 |
| **E-mail**     | gellihorvath@gmail.com |

---

## 2. Témaválasztás

|                     |                                     |
| ------------------- | ----------------------------------- |
| **Választott téma** | Platform-alapú játékipari analitika |

**Rövid leírás** *(2–4 mondat: milyen üzleti/elemzési kérdést old meg a pipeline? Milyen forrásadatokból indul ki, és milyen eredményt produkál?)*

> A pipeline célja, hogy platformok közötti összehasonlító játékipari elemzéseket fog végezni, különös tekintettel az eladások megoszlására, az árkülönbségekre és a műfajok elterjedtségére. Az alapadatokat a Kaggle Gaming Profiles 2025 CSV állományból fogom nyerni, amelyet kiegészítek a Steam JSON API-ból származó ár- és műfajadatokkal; ezeket előzetesen le fogom tölteni, és egy lokális API-n keresztül fogom kiszolgálni a lekérdezési korlátok miatt. Az így létrehozott, egységesített adathalmaz lehetővé fogja tenni a különböző platformok közötti mélyebb összehasonlító elemzést.
> 
> Megválaszolandó kérdések:
> 
> Egy adott játéknak hogy oszlanak el az eladásai az egyes platformokon (Steam vs Playstation és Xbox összes platform generáció)?
> 
> Átlagos árkülönbség a különböző platformokon lévő játékok árai között.
> 
> Egy adott műfaj eloszlása az egyes platformokon.

---

## 3. Tervezett pipeline elemei

| Elem                              | Tervezett megoldás / eszköz                                     |
| --------------------------------- | --------------------------------------------------------------- |
| **Adatforrások** *(min. 2)*       | JSON fájl + CSV fájl                                            |
| **Feldolgozási mód**              | Batch                                                           |
| **Landing zone** *(nyers tároló)* | lokális fájlrendszer                                            |
| **Adatmodell típusa**             | Csillag séma – 1 ténytábla + 2 dimenziótábla + 1 kapcsoló tábla |
| **Adattárház / adatplatform**     | PostgreSQL                                                      |
| **Transzformáció**                | Pandas                                                          |
| **Orchestration eszköz**          | Apache Airflow                                                  |
| **Infrastruktúra**                | Docker Compose                                                  |
| **Adatkiszolgálás**               | Metabase dashboard                                              |

---
