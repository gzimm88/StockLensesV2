
import React, { useState, useEffect, useMemo } from "react";
import { Ticker } from "@/api/entities";
import { Metrics } from "@/api/entities";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { 
  Target, 
  TrendingUp, 
  Calculator, 
  AlertTriangle,
  Download,
  RotateCcw
} from "lucide-react";
import {
  Tooltip as UITooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { 
  epsN, 
  priceN, 
  cagrFrom, 
  entryRequired, 
  peCurrentTrend,
  generatePricePath,
  generateTargetPath,
  buildPaths
} from "../components/utils/projections";
import { cleanBand, bandMid } from "../components/utils/peBand";
import { cacheLatestMosForTicker } from "../components/projections/getMos";

const SCENARIOS = {
  bear: "Bear Case",
  trend: "Current Trend", 
  bull: "Bull Case",
  constant: "Constant",
  custom: "Custom"
};


export default function Projection() {
  const [tickers, setTickers] = useState([]);
  const [selectedTickerSymbol, setSelectedTickerSymbol] = useState("");
  const [scenario, setScenario] = useState("trend");
  const [tableScenario, setTableScenario] = useState("trend");
  const [inputs, setInputs] = useState({
    priceToday: 100,
    EPS0: 5,
    growthRate: 0.15, // 15%
    years: 5,
    targetCAGR: 0.12, // 12%
    peBear: "", // Changed to empty string to indicate no default or auto-filled
    peBull: "", // Changed to empty string
    peMid: "", // Changed to empty string
    peTrend3y: 0.05 // 5% annual expansion
  });
  const [manualPE, setManualPE] = useState("");
  const [hasEditedPE, setHasEditedPE] = useState(false);
  const [peWarning, setPeWarning] = useState("");
  const [results, setResults] = useState(null);
  const [chartData, setChartData] = useState([]);
  const [yearlyData, setYearlyData] = useState(null);
  
  // New state for tracking P/E sources and edits
  const [peBandSources, setPeBandSources] = useState({
    bear: "manual", // "auto" | "manual"
    mid: "manual",
    bull: "manual"
  });
  const [peBandEdited, setPeBandEdited] = useState({
    bear: false,
    mid: false,
    bull: false
  });
  const [currentBand, setCurrentBand] = useState({ bear: null, mid: null, bull: null });

  const PE_now = useMemo(() => {
    if (manualPE === "" || manualPE === null || manualPE === undefined) return null;
    const pe = parseFloat(manualPE);
    if (isNaN(pe) || pe <= 0) return null;
    
    // Clamp to [1, 100] and set warning
    const clampedPE = Math.max(1, Math.min(100, pe));
    if (clampedPE !== pe && pe > 0) {
      setPeWarning("P/E clamped to [1–100]");
    } else {
      setPeWarning("");
    }
    
    return clampedPE;
  }, [manualPE]);

  // Validation for Bear vs Bull
  const bearBullWarning = useMemo(() => {
    const bear = parseFloat(inputs.peBear);
    const bull = parseFloat(inputs.peBull);
    if (!isNaN(bear) && !isNaN(bull) && bear > bull) {
      return "Bear P/E must be ≤ Bull P/E";
    }
    return "";
  }, [inputs.peBear, inputs.peBull]);

  useEffect(() => {
    loadTickers();
  }, []);

  useEffect(() => {
    calculateProjection();
  }, [inputs, scenario, PE_now]);

  useEffect(() => {
    if (yearlyData) {
      setTableScenario(scenario);
    }
  }, [scenario]);

  const loadTickers = async () => {
    try {
      const data = await Ticker.list();
      setTickers(data);
    } catch (error) {
      console.error("Error loading tickers:", error);
    }
  };

  const prefillPE = async (symbol) => {
    if (!symbol) {
      setManualPE("");
      return;
    }

    try {
      // Try to get from metrics first
      const metricsData = await Metrics.filter({ ticker_symbol: symbol });
      if (metricsData && metricsData.length > 0) {
        const metric = metricsData[0];
        if (metric.pe_ttm && metric.pe_ttm > 0) {
          setManualPE(metric.pe_ttm.toFixed(2).toString());
          return;
        }
      }
      
      // Fall back to Price/EPS calculation
      if (inputs.priceToday > 0 && inputs.EPS0 > 0) {
        const calculatedPE = (inputs.priceToday / inputs.EPS0).toFixed(2);
        setManualPE(calculatedPE);
      } else {
        setManualPE("");
      }
    } catch (error) {
      console.error("Error prefilling P/E:", error);
      // Fall back to Price/EPS calculation
      if (inputs.priceToday > 0 && inputs.EPS0 > 0) {
        const calculatedPE = (inputs.priceToday / inputs.EPS0).toFixed(2);
        setManualPE(calculatedPE);
      } else {
        setManualPE("");
      }
    }
  };

  const prefillPEBand = async (symbol) => {
    if (!symbol) {
      setCurrentBand({ bear: null, mid: null, bull: null });
      setPeBandSources({ bear: "manual", mid: "manual", bull: "manual" });
      setInputs(prev => ({ ...prev, peBear: "", peMid: "", peBull: "" }));
      return;
    }

    try {
      const metricsData = await Metrics.filter({ ticker_symbol: symbol });
      if (metricsData && metricsData.length > 0) {
        const metric = metricsData[0];
        const { low, high } = cleanBand(metric.pe_5y_low, metric.pe_5y_high, metric.pe_ttm);

        // Primary: use 5Y historical PE low/high from DB
        let bearPE = low;
        let bullPE = high;

        // Fallback: if 5Y PE band is missing, derive from pe_ttm (current PE)
        // Bear = current PE * 0.7, Bull = current PE * 1.3 (±30% spread)
        if ((bearPE == null || bullPE == null) && metric.pe_ttm && isFinite(metric.pe_ttm) && metric.pe_ttm > 0) {
          const basePE = metric.pe_ttm;
          bearPE = bearPE ?? parseFloat((basePE * 0.7).toFixed(1));
          bullPE = bullPE ?? parseFloat((basePE * 1.3).toFixed(1));
        }

        const midPE = bandMid(bearPE, bullPE);
        const band = { bear: bearPE, mid: midPE, bull: bullPE };
        setCurrentBand(band);

        const newInputs = { ...inputs };
        const newSources = { ...peBandSources };

        // Only update fields that haven't been manually edited in this session
        if (!peBandEdited.bear) {
          if (band.bear != null) {
            newInputs.peBear = band.bear.toFixed(1).toString();
            newSources.bear = low != null ? "auto" : "auto:estimated";
          } else {
            newInputs.peBear = "";
            newSources.bear = "manual";
          }
        }

        if (!peBandEdited.mid) {
          if (band.mid != null) {
            newInputs.peMid = band.mid.toFixed(1).toString();
            newSources.mid = "auto";
          } else {
            newInputs.peMid = "";
            newSources.mid = "manual";
          }
        }

        if (!peBandEdited.bull) {
          if (band.bull != null) {
            newInputs.peBull = band.bull.toFixed(1).toString();
            newSources.bull = high != null ? "auto" : "auto:estimated";
          } else {
            newInputs.peBull = "";
            newSources.bull = "manual";
          }
        }

        setInputs(newInputs);
        setPeBandSources(newSources);
      } else {
        setCurrentBand({ bear: null, mid: null, bull: null });
        setPeBandSources({ bear: "manual", mid: "manual", bull: "manual" });
        setInputs(prev => ({ ...prev, peBear: "", peMid: "", peBull: "" }));
      }
    } catch (error) {
      console.error("Error fetching P/E band:", error);
      setCurrentBand({ bear: null, mid: null, bull: null });
      setPeBandSources({ bear: "manual", mid: "manual", bull: "manual" });
      setInputs(prev => ({ ...prev, peBear: "", peMid: "", peBull: "" }));
    }
  };

  const handleTickerChange = async (symbol) => {
    setSelectedTickerSymbol(symbol);
    setHasEditedPE(false); // Reset edit flag for current PE
    setPeWarning(""); // Clear warnings for current PE
    
    // Reset P/E band edit flags and sources
    setPeBandEdited({ bear: false, mid: false, bull: false });
    setPeBandSources({ bear: "manual", mid: "manual", bull: "manual" });
    
    if (!symbol) {
      setInputs(prev => ({
        ...prev,
        EPS0: 5, // Reset EPS0 to a default if no ticker
        peBear: "",
        peBull: "",
        peMid: "",
      }));
      setCurrentBand({ bear: null, mid: null, bull: null });
      setManualPE("");
      return;
    }

    try {
      const metricsData = await Metrics.filter({ ticker_symbol: symbol });
      if (metricsData && metricsData.length > 0) {
        const metric = metricsData[0];
        const ttm = metric.pe_ttm;
        
        // This part needs currentPrice to calculate EPS, but currentPrice is in inputs.
        // If we update EPS0 based on TTM and a possibly stale price, it might be inaccurate.
        // For now, let's keep it as is, or consider updating priceToday first.
        // It's probably safer to let manualPE (prefilled) drive the PE, and EPS0 can be manually adjusted.
        // const price = inputs.priceToday;
        // const newEPS = ttm && price ? price/ttm : inputs.EPS0;
        // setInputs(prev => ({ ...prev, EPS0: newEPS }));

        // Only prefill EPS0 if it's currently 0 or not yet set by user
        if (inputs.EPS0 <= 0 || !hasEditedPE) { // Using hasEditedPE as a proxy for if EPS0 has been touched by user
          if (metric.eps_ttm && metric.eps_ttm > 0) {
            setInputs(prev => ({ ...prev, EPS0: metric.eps_ttm }));
          } else {
            setInputs(prev => ({ ...prev, EPS0: 5 })); // Default fallback
          }
        }

        // Also prefill current price, if available, and not already edited by user
        if (metric.price_current) {
          setInputs(prev => ({ ...prev, priceToday: metric.price_current }));
        }

      }
    } catch (error) {
      console.error("Error fetching metrics for ticker:", error);
    }

    // Prefill P/E after ticker change
    await prefillPE(symbol);
    await prefillPEBand(symbol);
  };

  const handleInputChange = (field, value) => {
    const numValue = parseFloat(value);
    setInputs(prev => ({ ...prev, [field]: isNaN(numValue) ? value : numValue })); // Allow empty string for number inputs
  };

  const handlePEBandChange = (field, value) => {
    // field can be 'bear', 'mid', 'bull'
    setInputs(prev => ({ ...prev, [`pe${field.charAt(0).toUpperCase() + field.slice(1)}`]: value }));
    setPeBandEdited(prev => ({ ...prev, [field]: true }));
    setPeBandSources(prev => ({ ...prev, [field]: "manual" }));
  };

  const handlePEChange = (value) => {
    setManualPE(value);
    setHasEditedPE(true);
  };

  const handlePEReset = async () => {
    setHasEditedPE(false);
    setPeWarning("");
    await prefillPE(selectedTickerSymbol);
  };

  const handlePEBandReset = (field) => {
    setPeBandEdited(prev => ({ ...prev, [field]: false }));
    
    if (currentBand[field] != null) {
      setInputs(prev => ({ ...prev, [`pe${field.charAt(0).toUpperCase() + field.slice(1)}`]: currentBand[field].toFixed(1).toString() }));
      setPeBandSources(prev => ({ ...prev, [field]: "auto" }));
    } else {
      setInputs(prev => ({ ...prev, [`pe${field.charAt(0).toUpperCase() + field.slice(1)}`]: "" }));
      setPeBandSources(prev => ({ ...prev, [field]: "manual" }));
    }
  };

  const calculateProjection = () => {
    if (inputs.EPS0 <= 0 || !PE_now || bearBullWarning) { // Also disable if P/E band warning exists
      setResults(null);
      setChartData([]);
      setYearlyData(null);
      return;
    }

    const { priceToday, EPS0, growthRate, years, targetCAGR, peTrend3y } = inputs;
    
    // Parse P/E values with validation and default fallbacks
    const bearPE = parseFloat(inputs.peBear) || 15;
    const bullPE = parseFloat(inputs.peBull) || 25;
    const midPE = parseFloat(inputs.peMid) || 20;

    // Ensure bearPE <= midPE <= bullPE for consistency, if they exist
    let finalBearPE = Math.min(bearPE, midPE, bullPE);
    let finalBullPE = Math.max(bearPE, midPE, bullPE);
    let finalMidPE = midPE;

    // Adjust mid if it's outside the final bear/bull range
    if (finalMidPE < finalBearPE) finalMidPE = finalBearPE;
    if (finalMidPE > finalBullPE) finalMidPE = finalBullPE;
    
    // Calculate yearly evolution data using buildPaths utility with manual PE_now
    const pathData = buildPaths({
      EPS0,
      gPct: growthRate * 100,
      N: years,
      priceToday,
      PE_bear: finalBearPE,
      PE_mid: finalMidPE,
      PE_bull: finalBullPE,
      PE_trendPct: peTrend3y * 100,
      PE_custom: finalMidPE, // 'custom' scenario often defaults to mid or constant
      targetCAGR: targetCAGR * 100,
      PE_now: PE_now,
      band: currentBand, // Pass the original metric band data
    });

    setYearlyData(pathData);

    const scenarioKey = scenario === 'trend' ? 'current' : scenario;
    const terminalData = pathData.terminal(scenarioKey);
    
    const terminalEPS = pathData.epsPath[pathData.epsPath.length - 1];
    const exitPE = pathData.pePaths[scenarioKey][pathData.pePaths[scenarioKey].length - 1];

    // Calculate margin of safety
    const marginOfSafety = (priceToday - terminalData.reqEntry) / terminalData.reqEntry;

    setResults({
      terminalEPS,
      exitPE,
      terminalPrice: terminalData.priceN,
      impliedCAGR: terminalData.impliedCAGR,
      requiredEntry: terminalData.reqEntry,
      upside: (terminalData.priceN - priceToday) / priceToday,
      marginOfSafety: marginOfSafety
    });
    
    // Cache MOS for the selected ticker if available
    if (selectedTickerSymbol && isFinite(marginOfSafety)) {
      cacheLatestMosForTicker(selectedTickerSymbol, marginOfSafety);
    }
    
    // Build chart data with multiple scenarios
    const selectedPricePath = pathData.pricePaths(scenarioKey);
    const targetPath = (targetCAGR && priceToday > 0) ? pathData.years.map(t => priceToday * Math.pow(1 + targetCAGR, t)) : [];
    
    const combined = pathData.years.map((year, index) => ({
      year,
      projected: selectedPricePath[index],
      target: targetPath[index] || 0
    }));
    
    setChartData(combined);
  };

  const exportYearlyData = () => {
    if (!yearlyData || !selectedTickerSymbol) return;

    const scenarioKey = tableScenario === "trend" ? "current" : tableScenario;
    const pricePath = yearlyData.pricePaths(scenarioKey);
    const targetPath = inputs.targetCAGR ? 
      yearlyData.years.map(t => inputs.priceToday * Math.pow(1 + inputs.targetCAGR, t)) : [];

    const headers = ['Year', 'EPS', 'PE', 'Price', 'TargetPath', 'PE_Bear_Source', 'PE_Mid_Source', 'PE_Bull_Source'];
    const rows = yearlyData.years.map((year, i) => [
      year,
      yearlyData.epsPath[i].toFixed(2),
      yearlyData.pePaths[scenarioKey][i].toFixed(1),
      pricePath[i].toFixed(2),
      targetPath[i] ? targetPath[i].toFixed(2) : '',
      peBandSources.bear,
      peBandSources.mid,
      peBandSources.bull
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedTickerSymbol || 'custom'}_yearly_projection_${tableScenario}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const formatCurrency = (value) => value != null && isFinite(value) ? `$${value.toFixed(2)}` : '--';
  const formatPercent = (value) => value != null && isFinite(value) ? `${(value * 100).toFixed(1)}%` : '--';

  const isValidProjection = inputs.EPS0 > 0 && PE_now > 0 && !bearBullWarning; // Include bearBullWarning in validity
  const hasWarning = inputs.EPS0 <= 0;
  const hasPEError = manualPE !== "" && PE_now === null;

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-slate-900">Stock Projection Tool</h1>
        <p className="text-slate-600 mt-1">
          Model future returns and calculate required entry prices
        </p>
      </div>

      <div className="grid lg:grid-cols-2 gap-8">
        {/* Input Panel */}
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Target className="w-5 h-5" />
                Stock Selection
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label>Select Ticker (Optional)</Label>
                <Select value={selectedTickerSymbol} onValueChange={handleTickerChange}>
                  <SelectTrigger>
                    <SelectValue placeholder="Choose a ticker or enter manually" />
                  </SelectTrigger>
                  <SelectContent>
                    {tickers.map(ticker => (
                      <SelectItem key={ticker.id} value={ticker.symbol}>
                        {ticker.symbol} - {ticker.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Calculator className="w-5 h-5" />
                Projection Inputs
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {hasWarning && (
                <div className="flex items-center gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-amber-800">
                  <AlertTriangle className="w-4 h-4" />
                  <span className="text-sm">EPS is ≤ 0. Switch to NTM EPS or owner earnings proxy to project.</span>
                </div>
              )}

              {peWarning && (
                <div className="flex items-center gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-amber-800">
                  <AlertTriangle className="w-4 h-4" />
                  <span className="text-sm">{peWarning}</span>
                </div>
              )}
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label>Current Price ($)</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={inputs.priceToday}
                    onChange={(e) => handleInputChange("priceToday", e.target.value)}
                  />
                </div>
                <div>
                  <Label>Current EPS ($)</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={inputs.EPS0}
                    onChange={(e) => handleInputChange("EPS0", e.target.value)}
                  />
                </div>
                <div>
                  <Label>EPS Growth Rate (%)</Label>
                  <Input
                    type="number"
                    step="1"
                    value={inputs.growthRate * 100}
                    onChange={(e) => handleInputChange("growthRate", e.target.value / 100)}
                  />
                </div>
                <div>
                  <Label>Time Horizon (Years)</Label>
                  <Input
                    type="number"
                    step="1"
                    min="1"
                    max="20"
                    value={inputs.years}
                    onChange={(e) => handleInputChange("years", e.target.value)}
                  />
                </div>
              </div>
              
              <div>
                <Label>Target CAGR (%)</Label>
                <Input
                  type="number"
                  step="1"
                  value={inputs.targetCAGR * 100}
                  onChange={(e) => handleInputChange("targetCAGR", e.target.value / 100)}
                />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>P/E Valuation Band</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {bearBullWarning && (
                <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-800">
                  <AlertTriangle className="w-4 h-4" />
                  <span className="text-sm">{bearBullWarning}</span>
                </div>
              )}
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <TooltipProvider>
                    <UITooltip>
                      <TooltipTrigger asChild>
                        <div>
                          <Label className="flex items-center gap-1">
                            Current P/E (Manual)
                            <div className="text-xs text-slate-500 cursor-help">?</div>
                          </Label>
                        </div>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Sets Year-0 P/E and the starting point for scenario evolution.</p>
                      </TooltipContent>
                    </UITooltip>
                  </TooltipProvider>
                  <div className="flex gap-2">
                    <Input
                      type="number"
                      step="0.01"
                      min="1"
                      max="100"
                      value={manualPE}
                      onChange={(e) => handlePEChange(e.target.value)}
                      placeholder="—"
                      className={hasPEError ? "border-red-500" : ""}
                    />
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handlePEReset}
                      className="shrink-0"
                    >
                      <RotateCcw className="w-4 h-4" />
                    </Button>
                  </div>
                  <p className="text-xs text-slate-500 mt-1">
                    Year-0 P/E. Prefilled from metrics or Price/EPS, but you can override.
                  </p>
                  {hasPEError && (
                    <p className="text-xs text-red-600 mt-1">Invalid P/E value</p>
                  )}
                </div>
                 <div>
                  <Label>P/E Trend (Annual % Change)</Label>
                  <Input
                    type="number"
                    step="1"
                    value={inputs.peTrend3y * 100}
                    onChange={(e) => handleInputChange("peTrend3y", e.target.value / 100)}
                  />
                </div>
              </div>
              
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <Label>Bear P/E</Label>
                  <div className="flex gap-2">
                    <Input
                      type="number"
                      step="0.1"
                      min="1"
                      max="100"
                      value={inputs.peBear}
                      onChange={(e) => handlePEBandChange("bear", e.target.value)}
                      placeholder={peBandSources.bear === "manual" ? "Manual required" : ""}
                    />
                    {peBandSources.bear === "auto" && (
                      <Button
                        variant="outline"
                        size="icon"
                        onClick={() => handlePEBandReset("bear")}
                        className="shrink-0"
                      >
                        <RotateCcw className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                  <div className="flex items-center gap-1 mt-1">
                    <Badge variant={peBandSources.bear !== "manual" ? "default" : "outline"} className="text-xs">
                      {peBandSources.bear === "auto" ? "Auto (5Y low)" : peBandSources.bear === "auto:estimated" ? "Auto (est. ±PE)" : "Manual"}
                    </Badge>
                  </div>
                </div>
                <div>
                  <Label>Mid P/E (Constant)</Label>
                  <div className="flex gap-2">
                    <Input
                      type="number"
                      step="0.1"
                      min="1"
                      max="100"
                      value={inputs.peMid}
                      onChange={(e) => handlePEBandChange("mid", e.target.value)}
                      placeholder={peBandSources.mid === "manual" ? "Manual required" : ""}
                    />
                    {peBandSources.mid !== "manual" && (
                      <Button
                        variant="outline"
                        size="icon"
                        onClick={() => handlePEBandReset("mid")}
                        className="shrink-0"
                      >
                        <RotateCcw className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                  <div className="flex items-center gap-1 mt-1">
                    <Badge variant={peBandSources.mid !== "manual" ? "default" : "outline"} className="text-xs">
                      {peBandSources.mid !== "manual" ? "Auto (avg)" : "Manual"}
                    </Badge>
                  </div>
                </div>
                <div>
                  <Label>Bull P/E</Label>
                  <div className="flex gap-2">
                    <Input
                      type="number"
                      step="0.1"
                      min="1"
                      max="100"
                      value={inputs.peBull}
                      onChange={(e) => handlePEBandChange("bull", e.target.value)}
                      placeholder={peBandSources.bull === "manual" ? "Manual required" : ""}
                    />
                    {peBandSources.bull !== "manual" && (
                      <Button
                        variant="outline"
                        size="icon"
                        onClick={() => handlePEBandReset("bull")}
                        className="shrink-0"
                      >
                        <RotateCcw className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                  <div className="flex items-center gap-1 mt-1">
                    <Badge variant={peBandSources.bull !== "manual" ? "default" : "outline"} className="text-xs">
                      {peBandSources.bull === "auto" ? "Auto (5Y high)" : peBandSources.bull === "auto:estimated" ? "Auto (est. ±PE)" : "Manual"}
                    </Badge>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Scenario</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-2">
                {Object.entries(SCENARIOS).map(([key, label]) => (
                  <Button
                    key={key}
                    variant={scenario === key ? "default" : "outline"}
                    onClick={() => setScenario(key)}
                    className="justify-start"
                  >
                    {label}
                  </Button>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Results Panel */}
        <div className="space-y-6">
          {(!isValidProjection || hasPEError || bearBullWarning) && (
            <Card className="border-amber-200 bg-amber-50">
              <CardContent className="pt-6">
                <div className="flex items-center gap-3 text-amber-800">
                  <AlertTriangle className="w-5 h-5" />
                  <p>
                    {hasPEError ? "Enter a valid Current P/E to see projections" : 
                     bearBullWarning ? bearBullWarning :
                     "Enter a positive EPS value and current P/E to see projections"}
                  </p>
                </div>
              </CardContent>
            </Card>
          )}

          {isValidProjection && !hasPEError && results && (
            <>
              {/* Results Summary */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <span>Projection Results</span>
                    <Badge variant="outline">{SCENARIOS[scenario]}</Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-6">
                    <div className="grid grid-cols-2 gap-4">
                      <div className="text-center p-4 bg-slate-50 rounded-lg">
                        <p className="text-sm text-slate-600">Terminal EPS</p>
                        <p className="text-2xl font-bold text-slate-900">
                          {formatCurrency(results.terminalEPS)}
                        </p>
                      </div>
                      <div className="text-center p-4 bg-slate-50 rounded-lg">
                        <p className="text-sm text-slate-600">Exit P/E</p>
                        <p className="text-2xl font-bold text-slate-900">
                          {results.exitPE.toFixed(1)}x
                        </p>
                      </div>
                    </div>

                    <div className="text-center p-6 bg-slate-900 text-white rounded-lg">
                      <p className="text-sm opacity-90">Terminal Price</p>
                      <p className="text-3xl font-bold">
                        {formatCurrency(results.terminalPrice)}
                      </p>
                      <p className="text-sm mt-2">
                        {formatPercent(results.upside)} upside
                      </p>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="text-center p-4 bg-emerald-50 rounded-lg">
                        <p className="text-sm text-emerald-700">Implied CAGR</p>
                        <p className="text-2xl font-bold text-emerald-900">
                          {formatPercent(results.impliedCAGR)}
                        </p>
                      </div>
                      <div className="text-center p-4 bg-blue-50 rounded-lg">
                        <p className="text-sm text-blue-700">Required Entry</p>
                        <p className="text-2xl font-bold text-blue-900">
                          {formatCurrency(results.requiredEntry)}
                        </p>
                      </div>
                    </div>

                    <div className="text-center p-4 bg-amber-50 rounded-lg">
                      <p className="text-sm text-amber-700">Margin of Safety</p>
                      <p className={`text-xl font-bold ${
                        results.marginOfSafety > 0 ? 'text-emerald-900' : 'text-red-900'
                      }`}>
                        {formatPercent(results.marginOfSafety)}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Price Chart */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="w-5 h-5" />
                    Price Projection vs Target (Yearly)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis 
                          dataKey="year" 
                          stroke="#64748b"
                          tick={{ fontSize: 12 }}
                        />
                        <YAxis 
                          stroke="#64748b"
                          tick={{ fontSize: 12 }}
                          tickFormatter={(value) => `$${value.toFixed(0)}`}
                        />
                        <Tooltip 
                          formatter={(value, name) => [
                            formatCurrency(value), 
                            name === 'projected' ? 'Projected Price' : 'Target Price'
                          ]}
                          labelFormatter={(label) => `Year ${label}`}
                          contentStyle={{
                            backgroundColor: '#f8fafc',
                            border: '1px solid #e2e8f0',
                            borderRadius: '8px'
                          }}
                        />
                        <Legend />
                        <Line 
                          type="monotone" 
                          dataKey="projected" 
                          stroke="#0f172a" 
                          strokeWidth={3}
                          name="Projected Path"
                          dot={{ fill: '#0f172a', r: 4 }}
                        />
                        <Line 
                          type="monotone" 
                          dataKey="target" 
                          stroke="#f59e0b" 
                          strokeWidth={2}
                          strokeDasharray="5 5"
                          name="Target Path"
                          dot={{ fill: '#f59e0b', r: 3 }}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              {/* Yearly Evolution Section */}
              {yearlyData && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      <span>Yearly Evolution</span>
                      <div className="flex items-center gap-2">
                        <Select value={tableScenario} onValueChange={setTableScenario}>
                          <SelectTrigger className="w-40">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {Object.entries(SCENARIOS).map(([key, label]) => (
                              <SelectItem key={key} value={key}>
                                {label}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <Button variant="outline" size="sm" onClick={exportYearlyData} className="gap-2">
                          <Download className="w-4 h-4" />
                          Export CSV
                        </Button>
                      </div>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="rounded-lg border overflow-hidden">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead>Year</TableHead>
                            <TableHead>EPS</TableHead>
                            <TableHead>P/E</TableHead>
                            <TableHead>Price</TableHead>
                            <TableHead>Target Path</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {yearlyData.years.map((year, i) => {
                            const scenarioKey = tableScenario === "trend" ? "current" : tableScenario;
                            const pricePath = yearlyData.pricePaths(scenarioKey);
                            const targetPrice = inputs.targetCAGR ? 
                              inputs.priceToday * Math.pow(1 + inputs.targetCAGR, year) : null;
                            
                            return (
                              <TableRow key={year}>
                                <TableCell className="font-medium">{year}</TableCell>
                                <TableCell className="font-mono">{formatCurrency(yearlyData.epsPath[i])}</TableCell>
                                <TableCell className="font-mono">{yearlyData.pePaths[scenarioKey][i].toFixed(1)}x</TableCell>
                                <TableCell className="font-mono font-medium">{formatCurrency(pricePath[i])}</TableCell>
                                <TableCell className="font-mono text-amber-600">
                                  {targetPrice ? formatCurrency(targetPrice) : '--'}
                                </TableCell>
                              </TableRow>
                            );
                          })}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
