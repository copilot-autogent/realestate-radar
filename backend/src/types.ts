/**
 * 實價登錄 open data types
 * Based on 內政部 plvr.land.moi.gov.tw CSV schema
 */

export interface PlvrRawRecord {
  鄉鎮市區: string;
  交易標的: string;
  土地位置建物門牌: string;
  土地移轉總面積平方公尺: string;
  都市土地使用分區: string;
  交易年月日: string;
  交易筆棟數: string;
  移轉層次: string;
  總樓層數: string;
  建物型態: string;
  主要用途: string;
  主要建材: string;
  建築完成年月: string;
  建物移轉總面積平方公尺: string;
  建物現況格局房: string;
  建物現況格局廳: string;
  建物現況格局衛: string;
  建物現況格局隔間: string;
  有無管理組織: string;
  總價元: string;
  單價元平方公尺: string;
  車位類別: string;
  車位移轉總面積平方公尺: string;
  車位總價元: string;
  備註: string;
  編號: string;
}

export interface Transaction {
  city: string;
  district: string;
  address: string | null;
  transactionDate: Date;
  transactionType: string;
  totalPrice: number;
  unitPrice: number | null;
  areaSqm: number | null;
  buildingType: string | null;
  floorsTotal: number | null;
  floor: string | null;
  buildYear: number | null;
  rooms: number | null;
  halls: number | null;
  bathrooms: number | null;
  hasParking: boolean;
  parkingType: string | null;
  parkingPrice: number | null;
  parkingArea: number | null;
  landUse: string | null;
  note: string | null;
  serialNumber: string;
}

/** ROC date string (e.g. "1130115") → JS Date */
export function parseRocDate(rocDate: string): Date | null {
  if (!rocDate || rocDate.length < 5) return null;
  const rocYear = parseInt(rocDate.substring(0, rocDate.length - 4), 10);
  const month = parseInt(rocDate.substring(rocDate.length - 4, rocDate.length - 2), 10);
  const day = parseInt(rocDate.substring(rocDate.length - 2), 10);
  if (isNaN(rocYear) || isNaN(month) || isNaN(day)) return null;
  const year = rocYear + 1911;
  return new Date(year, month - 1, day);
}

/** Unit price from 元/平方公尺 → 元/坪 */
export function sqmToPing(pricePerSqm: number): number {
  return Math.round(pricePerSqm * 3.30579);
}

// Taiwan city codes used in PLVR download URLs
export const CITY_CODES: Record<string, string> = {
  A: "台北市",
  B: "台中市",
  C: "基隆市",
  D: "台南市",
  E: "高雄市",
  F: "新北市",
  G: "宜蘭縣",
  H: "桃園市",
  I: "嘉義市",
  J: "新竹縣",
  K: "苗栗縣",
  L: "台中縣", // merged
  M: "南投縣",
  N: "彰化縣",
  O: "新竹市",
  P: "雲林縣",
  Q: "嘉義縣",
  R: "台南縣", // merged
  S: "高雄縣", // merged
  T: "屏東縣",
  U: "花蓮縣",
  V: "台東縣",
  W: "金門縣",
  X: "澎湖縣",
  Z: "連江縣",
};
