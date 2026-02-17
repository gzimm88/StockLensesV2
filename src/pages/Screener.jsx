import React, { useState, useEffect, useMemo } from "react";
import { Ticker } from "@/api/entities";
import { Metrics } from "@/api/entities";
import { LensPreset } from "@/api/entities";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { 
  Download, 
  Search, 
  TrendingUp, 
  ArrowUpDown,
  Filter,
  Upload,
  Bug
} from "lucide-react";
import { Link } from "react-router-dom";
import { createPageUrl } from "@/utils";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { Slider } from "@/components/ui/slider";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Label } from "@/components/ui/label";
import { toPoints, toNumber } from "../components/utils/num";
import { normalizeSymbol } from "../components/utils/normalizeSymbol";
import { getLatestMetricsBySymbol, deduplicateTickers } from "../components/utils/metricsView";
import { computeCategoryScores, computeFinalScore } from "../components/utils/scoring";
import { recommend } from "../components/scoring/recommend";
import { lensRec } from "../components/config/lenses";
import { getLatestMosForTicker } from "../components/projections/getMos";
import { cleanBand } from "../components/utils/peBand";
import ScoreTraceDrawer from "../components/debug/ScoreTraceDrawer";

const ScoreBreakdown = ({ name, score, details }) => (
    <Card>
        <CardHeader className="pb-2">
            <CardTitle className="text-base flex justify-between items-center">
                <span>{name}</span>
                <Badge variant="outline">{score?.toFixed(2) ?? 'N/A'}</Badge>
            </CardTitle>
        </CardHeader>
        <CardContent className="text-sm space-y-2">
            {Object.entries(details).map(([key, value]) => (
                <div key={key} className="flex justify-between items-center">
                    <span className="text-slate-600">{key}</span>
                    <span className="font-mono text-slate-800">{value ?? '--'}</span>
                </div>
            ))}
        </CardContent>
    </Card>
);

const ScoreTooltipContent = ({ scores, lens }) => (
    <div className="p-2 space-y-1">
        <p className="font-bold text-sm mb-2">Score Breakdown (Lens: {lens.name})</p>
        <div className="grid grid-cols-3 gap-x-4 gap-y-1 text-xs">
            {Object.entries(scores).map(([key, value]) => (
                <React.Fragment key={key}>
                    <span className="capitalize text-slate-600">{key.replace(/([A-Z])/g, ' $1')}</span>
                    <span className="font-mono text-right">{value.toFixed(2)}</span>
                    <span className="font-mono text-right text-slate-500">({lens[key] || 0}%)</span>
                </React.Fragment>
            ))}
        </div>
    </div>
);

const StockDetailDrawer = ({ ticker, metrics }) => {
  if (!ticker || !metrics) return null;

  const scores = computeCategoryScores(metrics);

  const breakdownDetails = {
      Valuation: {
          score: scores.valuation,
          details: {
              "PE Fwd": metrics.pe_fwd,
              "PE Fwd Sector": metrics.pe_fwd_sector,
              "PEG 5Y": metrics.peg_5y,
              "EV/EBITDA": metrics.ev_ebitda,
              "EV/EBITDA Sector": metrics.ev_ebitda_sector,
              "FCF Yield %": metrics.fcf_yield_pct,
              "Hist. PE TTM": metrics.pe_ttm,
              "Hist. PE 5Y Low": metrics.pe_5y_low,
              "Hist. PE 5Y High": metrics.pe_5y_high,
          }
      },
      Quality: {
          score: scores.quality,
          details: {
              "ROIC %": metrics.roic_pct,
              "FCF Margin %": metrics.fcf_margin_pct,
              "CFO/NI": metrics.cfo_to_ni,
              "FCF/EBIT": metrics.fcf_to_ebit,
              "Accruals Ratio": metrics.accruals_ratio,
              "Margin Stdev 5Y %": metrics.margin_stdev_5y_pct,
          }
      },
      "Capital Allocation": {
          score: scores.capitalAllocation,
          details: {
              "Buyback Yield %": metrics.buyback_yield_pct,
              "Debt/Equity": metrics.debt_to_equity,
              "Net Debt/EBITDA": metrics.netdebt_to_ebitda,
              "Interest Coverage": metrics.interest_coverage_x,
          }
      },
      Growth: {
          score: scores.growth,
          details: {
              "EPS CAGR 5Y %": metrics.eps_cagr_5y_pct,
              "Revenue CAGR 5Y %": metrics.revenue_cagr_5y_pct,
              "EPS CAGR 3Y %": metrics.eps_cagr_3y_pct,
              "Revenue CAGR 3Y %": metrics.rev_cagr_3y_pct,
          }
      },
      Moat: {
          score: scores.moat,
          details: {
              "Moat Score": metrics.moat_score_0_10,
              "Recurring Rev %": metrics.recurring_revenue_pct,
              "Insider Own %": metrics.insider_own_pct,
              "Founder Led": metrics.founder_led_bool ? "Yes" : "No",
          }
      },
      Risk: {
          score: scores.risk,
          details: {
              "Risk Score": metrics.riskdownside_score_0_10,
              "Net Debt/EBITDA": metrics.netdebt_to_ebitda,
              "Net Cash/MktCap %": metrics.netcash_to_mktcap_pct,
              "Beta 5Y": metrics.beta_5y,
              "Max Drawdown 5Y %": metrics.maxdrawdown_5y_pct,
              "Cyclicality": metrics.sector_cyc_tag,
          }
      },
      Macro: {
          score: scores.macro,
          details: { "Macro Fit Score": metrics.macrofit_score_0_10 }
      },
      Narrative: {
          score: scores.narrative,
          details: { "Narrative Score": metrics.narrative_score_0_10 }
      },
      Dilution: {
          score: scores.dilution,
          details: {
              "Share Count Change 5Y %": metrics.sharecount_change_5y_pct,
              "SBC to Sales %": metrics.sbc_to_sales_pct,
          }
      }
  };

  return (
    <SheetContent className="w-[400px] sm:w-[540px] overflow-y-auto">
      <SheetHeader>
        <SheetTitle>{ticker.name} ({ticker.symbol})</SheetTitle>
        <p className="text-sm text-slate-500">Scoring components breakdown</p>
      </SheetHeader>
      <div className="py-4 space-y-4">
        {Object.entries(breakdownDetails).map(([name, data]) => (
            <ScoreBreakdown key={name} name={name} score={data.score} details={data.details} />
        ))}
      </div>
    </SheetContent>
  );
};

const CSVImporter = ({ onImported }) => {
  const [isImporting, setIsImporting] = useState(false);

  const handleFileChange = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setIsImporting(true);
    const text = await file.text();
    const rows = text.split('\n');
    const headers = rows[0].split(',').map(h => h.trim());
    const dataRows = rows.slice(1);
    
    const tickersToUpsert = [];
    const metricsToUpsert = [];

    for (const row of dataRows) {
        if (!row.trim()) continue;
        const values = row.split(',');
        const rowData = headers.reduce((obj, header, index) => {
            const value = values[index]?.trim();
            obj[header] = value === '?' || value === '' ? null : value;
            return obj;
        }, {});
        
        const symbol = normalizeSymbol(rowData.symbol);
        if (!symbol) continue;

        // Ticker data
        tickersToUpsert.push({ 
          id: symbol, 
          symbol: symbol, 
          name: rowData.name,
          exchange: rowData.exchange || ''
        });

        // Metrics data with normalization
        const metricsData = { ticker_symbol: symbol };
        for (const key in rowData) {
            if (key !== 'symbol' && key !== 'name' && key !== 'exchange') {
                const value = rowData[key];
                if (key.endsWith('_pct') || key.endsWith('_pp')) {
                    metricsData[key] = toPoints(value);
                } else if (key === 'founder_led_bool') {
                    metricsData[key] = value?.toLowerCase() === 'true';
                } else {
                    metricsData[key] = toNumber(value);
                }
            }
        }
        metricsToUpsert.push(metricsData);
    }
    
    // Upsert Tickers
    for(const ticker of tickersToUpsert) {
        const existing = await Ticker.filter({ symbol: ticker.symbol });
        if (existing.length > 0) {
            await Ticker.update(existing[0].id, { name: ticker.name, exchange: ticker.exchange });
        } else {
            await Ticker.create(ticker);
        }
    }

    // Upsert Metrics
    for (const metric of metricsToUpsert) {
        const existing = await Metrics.filter({ ticker_symbol: metric.ticker_symbol });
        const { ticker_symbol, ...dataToSave } = metric;
        if (existing.length > 0) {
            await Metrics.update(existing[0].id, dataToSave);
        } else {
            await Metrics.create(metric);
        }
    }

    setIsImporting(false);
    onImported();
  };

  return (
    <>
      <Button variant="outline" className="gap-2" asChild>
        <label htmlFor="csv-importer">
          <Upload className="w-4 h-4" />
          {isImporting ? "Importing..." : "Import CSV"}
        </label>
      </Button>
      <input id="csv-importer" type="file" accept=".csv" className="hidden" onChange={handleFileChange} disabled={isImporting} />
    </>
  );
};

export default function Screener() {
  const [tickers, setTickers] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [lenses, setLenses] = useState([]);
  const [selectedLens, setSelectedLens] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [sortField, setSortField] = useState("finalScore");
  const [sortDirection, setSortDirection] = useState("desc");
  const [isLoading, setIsLoading] = useState(true);
  const [detailTicker, setDetailTicker] = useState(null);
  const [traceDrawerOpen, setTraceDrawerOpen] = useState(false);
  const [traceDrawerData, setTraceDrawerData] = useState(null);
  const [recommendationFilter, setRecommendationFilter] = useState("All");
  const [minScoreFilter, setMinScoreFilter] = useState(0);

  useEffect(() => {
    loadData();
  }, []);
  
  const loadData = async () => {
    setIsLoading(true);
    try {
      const [tickerData, metricsData, lensData] = await Promise.all([
        Ticker.list(),
        Metrics.list(),
        LensPreset.list()
      ]);

      // Apply deduplication utilities
      const uniqueTickers = deduplicateTickers(tickerData);
      const latestMetrics = getLatestMetricsBySymbol(metricsData);
      
      setTickers(uniqueTickers);
      setMetrics(latestMetrics);
      setLenses(lensData);
      
      if (lensData.length > 0 && !selectedLens) {
        setSelectedLens(lensData[0]);
      }
    } catch (error) {
      console.error("Error loading data:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const enrichedData = useMemo(() => {
    if (!selectedLens) return [];
    
    const data = tickers.map(ticker => {
      if (!ticker || !ticker.symbol) return null;
      
      const metric = metrics.find(m => m && m.ticker_symbol && normalizeSymbol(m.ticker_symbol) === normalizeSymbol(ticker.symbol));
      if (!metric) return null;
      
      const scores = computeCategoryScores(metric);
      const finalScore = computeFinalScore(scores, selectedLens);
      
      const mos = getLatestMosForTicker(ticker.symbol);
      const lensConfig = lensRec[selectedLens.name] || lensRec["Conservative"];
      
      const { rec, mosStatus } = recommend(finalScore, mos, lensConfig.mos);
      
      return {
        ticker,
        metrics: metric,
        scores,
        finalScore,
        recommendation: rec,
        mosStatus: mosStatus,
        mos,
      };
    }).filter(Boolean);
    
    // Filter by search term, recommendation, and min score
    const filtered = data.filter(item => {
        const searchMatch = !searchTerm || 
            (item.ticker.symbol && item.ticker.symbol.toLowerCase().includes(searchTerm.toLowerCase())) ||
            (item.ticker.name && item.ticker.name.toLowerCase().includes(searchTerm.toLowerCase()));
        
        const recommendationMatch = recommendationFilter === 'All' || item.recommendation === recommendationFilter;
        
        const scoreMatch = item.finalScore >= minScoreFilter;

        return searchMatch && recommendationMatch && scoreMatch;
    });
    
    // Sort data
    return filtered.sort((a, b) => {
      if (sortField === "recommendation") {
          const order = { BUY: 3, WATCH: 2, AVOID: 1 };
          const aVal = order[a.recommendation] ?? 0;
          const bVal = order[b.recommendation] ?? 0;
          if (aVal !== bVal) {
              return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
          }
          // Secondary sort by score if recommendations are the same
          return b.finalScore - a.finalScore;
      }

      let aVal = sortField === "finalScore" ? a.finalScore : 
                 sortField === "ticker" ? (a.ticker.symbol || '') :
                 a.scores[sortField] || 0;
      let bVal = sortField === "finalScore" ? b.finalScore : 
                 sortField === "ticker" ? (b.ticker.symbol || '') :
                 b.scores[sortField] || 0;
                 
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDirection === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      }
      
      return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
    });
  }, [tickers, metrics, selectedLens, searchTerm, sortField, sortDirection, recommendationFilter, minScoreFilter]);

  const handleSort = (field) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const downloadCSV = () => {
    if (enrichedData.length === 0 || !selectedLens) return;
    
    const lensConfig = lensRec[selectedLens.name] || lensRec["Conservative"];
    
    const headers = [
      'symbol', 'name', 'lens', 'recommendation', 'finalScore',
      'buy_threshold', 'watch_threshold', 'mos_threshold', 'mos_value', 'pe_low', 'pe_high', 'pe_ttm',
      'pe_band_source',
      ...Object.keys(enrichedData[0].scores),
      'exportedAt',
    ];
    
    const rows = enrichedData.map(item => {
      const { low, high, ttm } = cleanBand(item.metrics.pe_5y_low, item.metrics.pe_5y_high, item.metrics.pe_ttm);
      const peBandSource = (low !== null && high !== null) ? 'auto' : 'manual';
      
      return [
        item.ticker.symbol,
        item.ticker.name || '',
        selectedLens.name,
        item.recommendation,
        item.finalScore.toFixed(2),
        lensConfig.buy || 8.0,
        lensConfig.watch || 6.5,
        lensConfig.mos || 0,
        item.mos ? (item.mos * 100).toFixed(1) + '%' : 'N/A',
        low ?? '',
        high ?? '',
        ttm ?? '',
        peBandSource,
        ...Object.values(item.scores).map(s => s != null ? s.toFixed(2) : ''),
        new Date().toISOString(),
      ];
    });
    
    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `alphastock-screener-${selectedLens.name.replace(/\s+/g, '_')}-${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getScoreColor = (score) => {
    if (score >= 8) return "text-green-700 bg-green-50";
    if (score >= 6) return "text-amber-700 bg-amber-50";
    if (score >= 4) return "text-orange-700 bg-orange-50";
    return "text-red-700 bg-red-50";
  };
  
  const getRecommendationBadge = (recommendation) => {
      switch(recommendation) {
          case 'BUY': return "bg-green-600 hover:bg-green-700 text-white";
          case 'WATCH': return "border border-amber-500 text-amber-600 bg-amber-50";
          case 'AVOID': return "bg-red-100 text-red-700";
          default: return "bg-slate-100 text-slate-600";
      }
  };

  const handleRowClick = (item) => {
      setDetailTicker(item);
  };

  const handleTraceClick = (item, event) => {
    event.stopPropagation();
    setTraceDrawerData({ ticker: item.ticker, metrics: item.metrics, lens: selectedLens });
    setTraceDrawerOpen(true);
  };

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-8 bg-slate-200 rounded animate-pulse" />
        <div className="h-64 bg-slate-200 rounded animate-pulse" />
      </div>
    );
  }

  const lensConfig = selectedLens ? (lensRec[selectedLens.name] || lensRec["Conservative"]) : null;

  return (
    <Sheet onOpenChange={(open) => !open && setDetailTicker(null)}>
      <div className="space-y-8">
        {/* Header */}
        <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Stock Screener</h1>
            <p className="text-slate-600 mt-1">
              Analyze {enrichedData.length} stocks with deterministic scoring
            </p>
          </div>
          <div className="flex items-center gap-3">
            <CSVImporter onImported={loadData} />
            <Button variant="outline" onClick={downloadCSV} className="gap-2">
              <Download className="w-4 h-4" />
              Export CSV
            </Button>
            <Link to={createPageUrl("StockDetail")}>
              <Button className="gap-2 bg-slate-900 hover:bg-slate-800">
                <TrendingUp className="w-4 h-4" />
                Add Stock
              </Button>
            </Link>
          </div>
        </div>

        {/* Controls */}
        <Card>
          <CardContent className="pt-6 space-y-4">
            <div className="flex flex-col lg:flex-row gap-4">
              <div className="flex-1">
                <div className="relative">
                  <Search className="absolute left-3 top-3 h-4 w-4 text-slate-400" />
                  <Input
                    placeholder="Search by ticker or company name..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="pl-10"
                  />
                </div>
              </div>
              <div className="lg:w-72">
                <Select 
                  value={selectedLens?.id || ""} 
                  onValueChange={(value) => {
                    const lens = lenses.find(l => l.id === value);
                    setSelectedLens(lens);
                  }}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select investment lens" />
                  </SelectTrigger>
                  <SelectContent>
                    {lenses.map((lens) => (
                      <SelectItem key={lens.id} value={lens.id}>
                        {lens.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            {selectedLens && lensConfig && (
                <div className="p-4 bg-slate-50 rounded-lg flex flex-col md:flex-row items-start md:items-center gap-4 border">
                    <div className="flex-1 flex flex-wrap items-center gap-x-6 gap-y-2">
                         <div className="flex items-center gap-2">
                            <span className="text-sm font-medium text-slate-700">Filter by Rec:</span>
                            <div className="flex gap-1">
                                {["All", "BUY", "WATCH", "AVOID"].map((rec) => (
                                    <Button
                                        key={rec}
                                        variant={recommendationFilter === rec ? "default" : "outline"}
                                        size="sm"
                                        onClick={() => setRecommendationFilter(rec)}
                                        className={`text-xs ${
                                            rec === "BUY" && recommendationFilter !== rec ? "text-green-600" :
                                            rec === "WATCH" && recommendationFilter !== rec ? "text-amber-600" :
                                            rec === "AVOID" && recommendationFilter !== rec ? "text-red-600" : ""
                                        }`}
                                    >
                                        {rec}
                                    </Button>
                                ))}
                            </div>
                        </div>
                        <div className="flex items-center gap-3 w-full md:w-auto">
                            <Label htmlFor="min-score-slider" className="text-sm font-medium text-slate-700 whitespace-nowrap">Min Score: {minScoreFilter.toFixed(1)}</Label>
                            <Slider
                                id="min-score-slider"
                                min={0} max={10} step={0.1}
                                value={[minScoreFilter]}
                                onValueChange={(val) => setMinScoreFilter(val[0])}
                                className="w-full md:w-48"
                            />
                        </div>
                    </div>
                    <div className="text-xs text-slate-500 bg-white px-2 py-1 rounded border">
                        Buy ≥{(lensConfig.buy || 8.0).toFixed(1)} | Watch ≥{(lensConfig.watch || 6.5).toFixed(1)}{lensConfig.mos > 0 ? ` | MOS ≥${lensConfig.mos}%` : ''}
                    </div>
                </div>
            )}
          </CardContent>
        </Card>

        {/* Results */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Filter className="w-5 h-5" />
              Analysis Results
              {selectedLens && (
                <Badge variant="outline" className="ml-auto">
                  {selectedLens.name}
                </Badge>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="rounded-lg border overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="bg-slate-50">
                    <TableHead 
                      className="cursor-pointer hover:bg-slate-100 transition-colors"
                      onClick={() => handleSort("ticker")}
                    >
                      Ticker
                    </TableHead>
                    <TableHead>Company</TableHead>
                    <TableHead
                      className="cursor-pointer hover:bg-slate-100 transition-colors text-center"
                      onClick={() => handleSort("recommendation")}
                    >
                        Rec.
                    </TableHead>
                    <TableHead 
                      className="cursor-pointer hover:bg-slate-100 transition-colors text-center"
                      onClick={() => handleSort("finalScore")}
                    >
                      Score
                    </TableHead>
                    <TooltipProvider>
                      {[
                        { key: "valuation", name: "Val", tooltip: "Relative, PEG, FCF Yield..." },
                        { key: "quality", name: "Qual", tooltip: "ROIC, Margins, Cash Conv..." },
                        { key: "capitalAllocation", name: "Cap", tooltip: "Buybacks, Debt, ROIIC..." },
                        { key: "growth", name: "Growth", tooltip: "CAGR, Acceleration..." },
                        { key: "moat", name: "Moat", tooltip: "Recurring Rev, Ownership..." },
                        { key: "risk", name: "Risk", tooltip: "Debt, Beta, Drawdown..." },
                      ].map(cat => (
                        <Tooltip key={cat.key} delayDuration={100}>
                          <TooltipTrigger asChild>
                            <TableHead 
                                className="cursor-pointer hover:bg-slate-100 transition-colors text-center"
                                onClick={() => handleSort(cat.key)}
                            >
                                {cat.name}
                                {sortField === cat.key && (
                                    <ArrowUpDown className={`inline-block ml-1 h-3 w-3 ${sortDirection === 'asc' ? 'rotate-180' : ''}`} />
                                )}
                            </TableHead>
                          </TooltipTrigger>
                          <TooltipContent><p>{cat.tooltip}</p></TooltipContent>
                        </Tooltip>
                      ))}
                    </TooltipProvider>
                    <TableHead className="text-right">PE Fwd</TableHead>
                    <TableHead className="text-right">FCF Yield</TableHead>
                    <TableHead className="text-center">Trace</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {enrichedData.map((item) => (
                    <SheetTrigger asChild key={item.ticker.symbol}>
                      <TableRow className="hover:bg-slate-50 cursor-pointer" onClick={() => handleRowClick(item)}>
                        <TableCell className="font-mono font-medium text-slate-900">{item.ticker.symbol}</TableCell>
                        <TableCell>
                          <p className="font-medium text-slate-900">{item.ticker.name}</p>
                        </TableCell>
                        <TableCell className="text-center">
                            <TooltipProvider>
                                <Tooltip delayDuration={100}>
                                    <TooltipTrigger asChild>
                                        <div className="flex items-center justify-center gap-1">
                                            <Badge className={`${getRecommendationBadge(item.recommendation)} font-semibold text-xs cursor-help`}>
                                                {item.recommendation}
                                            </Badge>
                                            {item.mosStatus && (
                                                <Badge variant="outline" className="text-xs px-1.5 py-0.5">{item.mosStatus}</Badge>
                                            )}
                                        </div>
                                    </TooltipTrigger>
                                    <TooltipContent>
                                        <div className="text-xs">
                                            <p className="font-semibold">Based on {selectedLens.name}</p>
                                            {lensConfig && (
                                                <p>Buy≥{(lensConfig.buy || 8.0).toFixed(1)}, Watch≥{(lensConfig.watch || 6.5).toFixed(1)}{lensConfig.mos > 0 ? `, MOS≥${lensConfig.mos}%` : ''}</p>
                                            )}
                                            {item.mos !== null && (
                                                <p className="mt-1 text-slate-600">
                                                    Current MOS: {(item.mos * 100).toFixed(1)}%
                                                </p>
                                            )}
                                        </div>
                                    </TooltipContent>
                                </Tooltip>
                            </TooltipProvider>
                        </TableCell>
                        <TableCell className="text-center">
                          <TooltipProvider>
                            <Tooltip delayDuration={100}>
                                <TooltipTrigger asChild>
                                    <Badge className={`font-mono ${getScoreColor(item.finalScore)}`}>
                                        {item.finalScore.toFixed(2)}
                                    </Badge>
                                </TooltipTrigger>
                                {selectedLens && <TooltipContent><ScoreTooltipContent scores={item.scores} lens={selectedLens} /></TooltipContent>}
                            </Tooltip>
                          </TooltipProvider>
                        </TableCell>
                        {Object.keys(item.scores).filter(k => ["valuation", "quality", "capitalAllocation", "growth", "moat", "risk"].includes(k)).map((key) => (
                          <TableCell key={key} className="text-center">
                            <span className={`px-2 py-1 rounded text-xs font-medium ${
                              item.scores[key] != null ? getScoreColor(item.scores[key]) : 'text-slate-400'
                            }`}>
                              {item.scores[key] != null ? item.scores[key].toFixed(1) : '--'}
                            </span>
                          </TableCell>
                        ))}
                        <TableCell className="text-right font-mono">
                          {item.metrics.pe_fwd != null ? item.metrics.pe_fwd.toFixed(1) : '--'}
                        </TableCell>
                        <TableCell className="text-right font-mono">
                          {item.metrics.fcf_yield_pct ? `${item.metrics.fcf_yield_pct.toFixed(1)}%` : '--'}
                        </TableCell>
                        <TableCell className="text-center">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={(e) => handleTraceClick(item, e)}
                            className="h-8 w-8 p-0"
                          >
                            <Bug className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    </SheetTrigger>
                  ))}
                </TableBody>
              </Table>
            </div>
            
            {enrichedData.length === 0 && (
              <div className="p-8 text-center text-slate-500">
                <TrendingUp className="w-12 h-12 mx-auto mb-4 text-slate-300" />
                <p className="text-lg font-medium">No stocks found</p>
                <p>Try adjusting your search criteria or add some stocks to analyze</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
      {detailTicker && <StockDetailDrawer ticker={detailTicker.ticker} metrics={detailTicker.metrics} />}
      {traceDrawerData && (
        <ScoreTraceDrawer
          ticker={traceDrawerData.ticker}
          metrics={traceDrawerData.metrics}
          lens={traceDrawerData.lens}
          isOpen={traceDrawerOpen}
          onClose={() => setTraceDrawerOpen(false)}
        />
      )}
    </Sheet>
  );
}