// Generate realistic sample transaction data for Taipei
import { writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const districts = [
  { name: "大安區", center: [121.5434, 25.0263], priceRange: [60, 120], weight: 15 },
  { name: "信義區", center: [121.5676, 25.0330], priceRange: [65, 130], weight: 12 },
  { name: "中山區", center: [121.5330, 25.0640], priceRange: [50, 100], weight: 12 },
  { name: "松山區", center: [121.5575, 25.0500], priceRange: [50, 95], weight: 10 },
  { name: "內湖區", center: [121.5880, 25.0700], priceRange: [35, 70], weight: 10 },
  { name: "士林區", center: [121.5250, 25.0930], priceRange: [30, 65], weight: 8 },
  { name: "北投區", center: [121.5020, 25.1320], priceRange: [25, 55], weight: 6 },
  { name: "萬華區", center: [121.4970, 25.0350], priceRange: [30, 55], weight: 6 },
  { name: "中正區", center: [121.5180, 25.0320], priceRange: [50, 90], weight: 8 },
  { name: "文山區", center: [121.5700, 25.0000], priceRange: [25, 50], weight: 6 },
  { name: "南港區", center: [121.6060, 25.0550], priceRange: [35, 65], weight: 5 },
  { name: "大同區", center: [121.5130, 25.0630], priceRange: [35, 60], weight: 5 },
];

const buildingTypes = ["住宅大樓", "華廈", "公寓", "透天厝", "套房"];
const typeWeights = [0.4, 0.2, 0.25, 0.05, 0.1];

function pick(arr, weights) {
  const r = Math.random();
  let sum = 0;
  for (let i = 0; i < arr.length; i++) {
    sum += weights[i];
    if (r < sum) return arr[i];
  }
  return arr[arr.length - 1];
}

function rand(min, max) { return min + Math.random() * (max - min); }
function randInt(min, max) { return Math.floor(rand(min, max)); }

const features = [];
let id = 1;

for (const dist of districts) {
  const count = dist.weight * 5;
  for (let i = 0; i < count; i++) {
    const unitPriceWan = rand(dist.priceRange[0], dist.priceRange[1]);
    const unitPrice = Math.round(unitPriceWan * 10000);
    const areaPing = rand(8, 55);
    const totalPrice = Math.round(unitPrice * areaPing);
    const btype = pick(buildingTypes, typeWeights);
    const floors = btype === "公寓" ? randInt(3, 6)
      : btype === "住宅大樓" ? randInt(10, 30)
      : btype === "華廈" ? randInt(6, 12)
      : randInt(1, 4);
    const floor = String(randInt(1, floors + 1));
    const rooms = btype === "套房" ? 1 : randInt(1, 5);

    const year = Math.random() > 0.4 ? 2025 : 2024;
    const month = randInt(1, 13);
    const day = randInt(1, 29);
    const date = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;

    const lon = dist.center[0] + (Math.random() - 0.5) * 0.03;
    const lat = dist.center[1] + (Math.random() - 0.5) * 0.02;

    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [+lon.toFixed(6), +lat.toFixed(6)] },
      properties: {
        id: id++,
        unitPrice,
        totalPrice,
        areaPing: +areaPing.toFixed(1),
        buildingType: btype,
        date,
        address: `${dist.name}某路${randInt(1, 200)}號${floor}樓`,
        city: "台北市",
        district: dist.name,
        floor,
        floorsTotal: floors,
        rooms,
      },
    });
  }
}

const geojson = { type: "FeatureCollection", features };
const outPath = resolve(__dirname, "../frontend/public/data/sample-transactions.json");
writeFileSync(outPath, JSON.stringify(geojson));
console.log(`Generated ${features.length} sample transactions → ${outPath}`);
