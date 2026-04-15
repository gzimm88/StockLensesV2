import React from "react";
import { geoNaturalEarth1, geoPath, geoCircle } from "d3-geo";
import { feature } from "topojson-client";
import { useTheme } from "next-themes";

const GEO_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

// Major exchange cities only
const CITIES = [
  { name: "New York", lon: -74.006, lat: 40.7128, tzOffset: -4, anchor: "end", dx: -10 },
  { name: "Frankfurt", lon: 8.6821, lat: 50.1109, tzOffset: 2, anchor: "start", dx: 10 },
  { name: "Hong Kong", lon: 114.1694, lat: 22.3193, tzOffset: 8, anchor: "end", dx: -10 },
];

const WIDTH = 960;
const HEIGHT = 400;

const projection = geoNaturalEarth1()
  .scale(148)
  .center([30, 15])
  .translate([WIDTH / 2, HEIGHT / 2]);

const pathGenerator = geoPath().projection(projection);

function getLocalTime(tzOffset) {
  const now = new Date();
  const utcMs = now.getTime() + now.getTimezoneOffset() * 60000;
  const local = new Date(utcMs + tzOffset * 3600000);
  return local.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

// Theme palettes
const THEMES = {
  light: {
    ocean: "#e8eef4",
    land: "#c8d6e0",
    landStroke: "#a0b4c4",
    nightOverlay: "rgba(30,41,59,0.3)",
    terminatorGlow: "rgba(245,158,11,0.2)",
    gridStroke: "#b0c4d4",
    cityDot: "#ea580c",
    cityGlow: "rgba(234,88,12,0.25)",
    cityName: "#1e293b",
    cityTime: "#475569",
  },
  dark: {
    ocean: "#0f172a",
    land: "#1e293b",
    landStroke: "#334155",
    nightOverlay: "rgba(0,0,0,0.4)",
    terminatorGlow: "rgba(251,191,36,0.15)",
    gridStroke: "#64748b",
    cityDot: "#f59e0b",
    cityGlow: "rgba(245,158,11,0.25)",
    cityName: "#e2e8f0",
    cityTime: "#94a3b8",
  },
};

export default function WorldMapSvg() {
  const { theme, resolvedTheme } = useTheme();
  const [geoData, setGeoData] = React.useState(null);
  const [times, setTimes] = React.useState({});
  const [nightPath, setNightPath] = React.useState("");

  const isDark = (resolvedTheme || theme) === "dark";
  const t = isDark ? THEMES.dark : THEMES.light;

  // Fetch world topology once
  React.useEffect(() => {
    fetch(GEO_URL)
      .then((r) => r.json())
      .then((topo) => {
        const countries = feature(topo, topo.objects.countries);
        setGeoData(countries);
      })
      .catch(() => {});
  }, []);

  // Update clocks and terminator every minute
  React.useEffect(() => {
    const update = () => {
      const newTimes = {};
      CITIES.forEach((c) => {
        newTimes[c.name] = getLocalTime(c.tzOffset);
      });
      setTimes(newTimes);

      // Subsolar point: where the sun is directly overhead right now
      const now = new Date();
      const dayOfYear = Math.floor(
        (now - new Date(now.getUTCFullYear(), 0, 0)) / 86400000
      );
      // Solar declination (approximate) in degrees
      const declination =
        -23.44 * Math.cos((2 * Math.PI * (dayOfYear + 10)) / 365);
      const hours = now.getUTCHours() + now.getUTCMinutes() / 60;
      const subSolarLon = -(hours - 12) * 15;

      // Anti-solar point = center of the night hemisphere (180° from the sun)
      let antiLon = subSolarLon + 180;
      if (antiLon > 180) antiLon -= 360;
      const antiLat = -declination;

      // Build the night hemisphere as a proper spherical polygon.
      // geoCircle centered on the anti-solar point with radius 90° = exactly the night half of Earth.
      // d3-geo handles anti-meridian clipping and projection correctly.
      const nightGeo = geoCircle()
        .center([antiLon, antiLat])
        .radius(90)
        .precision(1)();

      const d = pathGenerator(nightGeo);
      setNightPath(d || "");
    };
    update();
    const iv = setInterval(update, 60000);
    return () => clearInterval(iv);
  }, []);

  return (
    <div
      className="w-full overflow-hidden rounded-lg"
      style={{ backgroundColor: t.ocean }}
    >
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="w-full h-auto"
        style={{ display: "block" }}
      >
        {/* Ocean background */}
        <rect width={WIDTH} height={HEIGHT} fill={t.ocean} />

        {/* Subtle grid */}
        <g opacity="0.08" stroke={t.gridStroke} strokeWidth="0.3" fill="none">
          {[-60, -30, 0, 30, 60].map((lat) => {
            const d = pathGenerator({
              type: "LineString",
              coordinates: Array.from({ length: 361 }, (_, i) => [i - 180, lat]),
            });
            return d ? <path key={`lat${lat}`} d={d} /> : null;
          })}
          {[-150, -120, -90, -60, -30, 0, 30, 60, 90, 120, 150].map((lon) => {
            const d = pathGenerator({
              type: "LineString",
              coordinates: Array.from({ length: 181 }, (_, i) => [lon, i - 90]),
            });
            return d ? <path key={`lon${lon}`} d={d} /> : null;
          })}
        </g>

        {/* Country geometries */}
        {geoData &&
          geoData.features.map((feat, i) => {
            const d = pathGenerator(feat);
            return d ? (
              <path
                key={i}
                d={d}
                fill={t.land}
                stroke={t.landStroke}
                strokeWidth={0.4}
              />
            ) : null;
          })}

        {/* Night shadow — only covers the dark hemisphere */}
        {nightPath && (
          <>
            <path d={nightPath} fill={t.nightOverlay} />
            <path
              d={nightPath}
              fill="none"
              stroke={t.terminatorGlow}
              strokeWidth="1.5"
            />
          </>
        )}

        {/* City markers */}
        {CITIES.map((city) => {
          const projected = projection([city.lon, city.lat]);
          if (!projected) return null;
          const [cx, cy] = projected;
          const dx = city.dx || 10;
          const anchor = city.anchor || "start";
          return (
            <g key={city.name}>
              <circle cx={cx} cy={cy} r={6} fill={t.cityGlow} />
              <circle cx={cx} cy={cy} r={3.5} fill={t.cityDot} />
              <text
                x={cx + dx}
                y={cy - 4}
                textAnchor={anchor}
                fill={t.cityName}
                fontSize="13"
                fontWeight="700"
                fontFamily="system-ui, -apple-system, sans-serif"
              >
                {city.name}
              </text>
              <text
                x={cx + dx}
                y={cy + 10}
                textAnchor={anchor}
                fill={t.cityTime}
                fontSize="11"
                fontWeight="500"
                fontFamily="ui-monospace, SFMono-Regular, monospace"
              >
                {times[city.name] || "--:--"}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
