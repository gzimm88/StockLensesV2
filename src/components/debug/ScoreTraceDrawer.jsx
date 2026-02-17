import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { computeCategoryScores, computeFinalScore } from "../utils/scoring";
import { nz, toPoints } from "../utils/num";

const CategoryTraceCard = ({ categoryName, metrics, categoryScore, lens }) => {
  const getValuationTrace = (m) => {
    const pe_fwd = nz(m.pe_fwd);
    const pe_fwd_sector = nz(m.pe_fwd_sector);
    const peg_5y = nz(m.peg_5y);
    const ev_ebitda = nz(m.ev_ebitda);
    const ev_ebitda_sector = nz(m.ev_ebitda_sector);
    const fcf_yield_pct = toPoints(m.fcf_yield_pct);
    const pe_ttm = nz(m.pe_ttm);
    const pe_5y_low = nz(m.pe_5y_low);
    const pe_5y_high = nz(m.pe_5y_high);

    const pe_rel = (pe_fwd && pe_fwd_sector && pe_fwd_sector > 0) ? 
      10 * Math.max(0, Math.min(1, (1.5 - pe_fwd/pe_fwd_sector) / 0.8)) : null;
    const peg = peg_5y ? 10 * Math.max(0, Math.min(1, (2 - peg_5y) / 1.5)) : null;
    const ev_rel = (ev_ebitda && ev_ebitda_sector && ev_ebitda_sector > 0) ? 
      10 * Math.max(0, Math.min(1, (1.6 - ev_ebitda/ev_ebitda_sector) / 0.9)) : null;
    const fcf_yield = fcf_yield_pct ? Math.max(0, Math.min(10, fcf_yield_pct / 1.5)) : null;
    const pe_hist = (pe_ttm && pe_5y_low && pe_5y_high && pe_5y_high > pe_5y_low) ? 
      10 * Math.max(0, Math.min(1, (pe_5y_high - pe_ttm) / (pe_5y_high - pe_5y_low))) : null;

    return [
      { name: "PE Relative", raw: pe_fwd && pe_fwd_sector ? `${pe_fwd}/${pe_fwd_sector}` : "N/A", score: pe_rel, weight: "35%" },
      { name: "PEG 5Y", raw: peg_5y || "N/A", score: peg, weight: "20%" },
      { name: "EV/EBITDA Rel", raw: ev_ebitda && ev_ebitda_sector ? `${ev_ebitda}/${ev_ebitda_sector}` : "N/A", score: ev_rel, weight: "15%" },
      { name: "FCF Yield", raw: fcf_yield_pct ? `${fcf_yield_pct.toFixed(1)}%` : "N/A", score: fcf_yield, weight: "15%" },
      { name: "PE Historical", raw: pe_ttm || "N/A", score: pe_hist, weight: "15%" }
    ];
  };

  const getQualityTrace = (m) => {
    const roic_pct = toPoints(m.roic_pct);
    const fcf_margin_pct = toPoints(m.fcf_margin_pct);
    const cfo_to_ni = nz(m.cfo_to_ni);
    const fcf_to_ebit = nz(m.fcf_to_ebit);
    const accruals_ratio = nz(m.accruals_ratio);
    const margin_stdev_5y_pct = toPoints(m.margin_stdev_5y_pct);

    const roic = roic_pct ? Math.max(0, Math.min(10, roic_pct / 2)) : null;
    const fcf_margin = fcf_margin_pct ? Math.max(0, Math.min(10, fcf_margin_pct / 1.5)) : null;
    const cash_conv_vals = [cfo_to_ni, fcf_to_ebit].filter(v => v != null);
    const cash_conversion = cash_conv_vals.length ? Math.max(0, Math.min(10, 10 * (cash_conv_vals.reduce((a,b) => a+b, 0) / cash_conv_vals.length))) : null;
    const accruals = accruals_ratio == null ? 10 : 10 * Math.max(0, Math.min(1, (0.10 - Math.abs(accruals_ratio)) / 0.10));
    const margin_stability = margin_stdev_5y_pct ? 10 - Math.max(0, Math.min(10, margin_stdev_5y_pct * 0.5)) : null;

    return [
      { name: "ROIC", raw: roic_pct ? `${roic_pct.toFixed(1)}%` : "N/A", score: roic, weight: "20%" },
      { name: "FCF Margin", raw: fcf_margin_pct ? `${fcf_margin_pct.toFixed(1)}%` : "N/A", score: fcf_margin, weight: "20%" },
      { name: "Cash Conversion", raw: cash_conv_vals.length ? cash_conv_vals.map(v => v.toFixed(2)).join(", ") : "N/A", score: cash_conversion, weight: "20%" },
      { name: "Accruals", raw: accruals_ratio != null ? accruals_ratio.toFixed(3) : "N/A", score: accruals, weight: "20%" },
      { name: "Margin Stability", raw: margin_stdev_5y_pct ? `${margin_stdev_5y_pct.toFixed(1)}%` : "N/A", score: margin_stability, weight: "20%" }
    ];
  };

  const getCapitalAllocationTrace = (m) => {
    const buyback_yield_pct = toPoints(m.buyback_yield_pct);
    const interest_coverage_x = nz(m.interest_coverage_x);
    const ebit_t = nz(m.ebit_t);
    const ebit_t3 = nz(m.ebit_t3);
    const invcap_t = nz(m.invcap_t);
    const invcap_t3 = nz(m.invcap_t3);

    const buyback = buyback_yield_pct ? Math.max(0, Math.min(10, (buyback_yield_pct + 2) / 2)) : null;
    const interest_cover = interest_coverage_x ? Math.max(0, Math.min(10, Math.log10(interest_coverage_x + 1) * 4)) : null;
    const roiic = (ebit_t != null && ebit_t3 != null && invcap_t != null && invcap_t3 != null && invcap_t !== invcap_t3) ? 
      Math.max(0, Math.min(10, ((ebit_t - ebit_t3) / (invcap_t - invcap_t3)) * 100 / 2)) : null;

    return [
      { name: "Buyback Yield", raw: buyback_yield_pct ? `${buyback_yield_pct.toFixed(1)}%` : "N/A", score: buyback, weight: "33%" },
      { name: "Interest Coverage", raw: interest_coverage_x || "N/A", score: interest_cover, weight: "33%" },
      { name: "ROIIC Proxy", raw: roiic != null ? "Calculated" : "N/A", score: roiic, weight: "33%" }
    ];
  };

  const getGrowthTrace = (m) => {
    const eps_cagr_5y_pct = toPoints(m.eps_cagr_5y_pct);
    const revenue_cagr_5y_pct = toPoints(m.revenue_cagr_5y_pct);
    const eps_cagr_3y_pct = toPoints(m.eps_cagr_3y_pct);
    const rev_cagr_3y_pct = toPoints(m.rev_cagr_3y_pct);

    const eps5y = eps_cagr_5y_pct ? Math.max(0, Math.min(10, eps_cagr_5y_pct / 2)) : null;
    const rev5y = revenue_cagr_5y_pct ? Math.max(0, Math.min(10, revenue_cagr_5y_pct / 2)) : null;
    const accel = (eps_cagr_3y_pct != null && eps_cagr_5y_pct != null && rev_cagr_3y_pct != null && revenue_cagr_5y_pct != null) ? 
      Math.max(0, Math.min(10, 5 + 0.5 * ((eps_cagr_3y_pct - eps_cagr_5y_pct) + (rev_cagr_3y_pct - revenue_cagr_5y_pct)))) : null;
    const stage = revenue_cagr_5y_pct != null ? 
      (revenue_cagr_5y_pct >= 25 ? 10 : revenue_cagr_5y_pct >= 15 ? 8 : revenue_cagr_5y_pct >= 5 ? 6 : 3) : null;

    return [
      { name: "EPS CAGR 5Y", raw: eps_cagr_5y_pct ? `${eps_cagr_5y_pct.toFixed(1)}%` : "N/A", score: eps5y, weight: "25%" },
      { name: "Rev CAGR 5Y", raw: revenue_cagr_5y_pct ? `${revenue_cagr_5y_pct.toFixed(1)}%` : "N/A", score: rev5y, weight: "25%" },
      { name: "Acceleration", raw: accel != null ? "Calculated" : "N/A", score: accel, weight: "25%" },
      { name: "Stage Tag", raw: revenue_cagr_5y_pct ? `${revenue_cagr_5y_pct.toFixed(1)}%` : "N/A", score: stage, weight: "25%" }
    ];
  };

  const getMoatTrace = (m) => {
    const moat_score_0_10 = nz(m.moat_score_0_10);
    const recurring_revenue_pct = toPoints(m.recurring_revenue_pct);
    const insider_own_pct = toPoints(m.insider_own_pct);
    const founder_led_bool = m.founder_led_bool;

    const base_moat = moat_score_0_10;
    const recurring = recurring_revenue_pct ? 10 * Math.max(0, Math.min(1, recurring_revenue_pct / 100)) : null;
    const owner_block = Math.min(2, 10 * Math.max(0, Math.min(1, (insider_own_pct || 0) / 100))) + (founder_led_bool ? 1 : 0);

    return [
      { name: "Base Moat", raw: moat_score_0_10 || "N/A", score: base_moat, weight: "33%" },
      { name: "Recurring Revenue", raw: recurring_revenue_pct ? `${recurring_revenue_pct.toFixed(1)}%` : "N/A", score: recurring, weight: "33%" },
      { name: "Owner Block", raw: `${insider_own_pct || 0}% + ${founder_led_bool ? 'Founder' : 'No Founder'}`, score: owner_block, weight: "33%" }
    ];
  };

  const getRiskTrace = (m) => {
    const riskdownside_score_0_10 = nz(m.riskdownside_score_0_10);
    const netdebt_to_ebitda = nz(m.netdebt_to_ebitda);
    const netcash_to_mktcap_pct = toPoints(m.netcash_to_mktcap_pct);
    const beta_5y = nz(m.beta_5y);
    const maxdrawdown_5y_pct = toPoints(m.maxdrawdown_5y_pct);
    const sector_cyc_tag = m.sector_cyc_tag;

    const base_risk = riskdownside_score_0_10;
    const net_debt_ebitda = netdebt_to_ebitda ? 10 * Math.max(0, Math.min(1, (3 - netdebt_to_ebitda) / 2)) : null;
    const net_cash_mcap = Math.max(0, Math.min(10, 5 + Math.max(0, netcash_to_mktcap_pct || 0) / 2));
    const beta = beta_5y ? 10 - Math.max(0, Math.min(10, Math.abs(beta_5y) * 5)) : null;
    const max_drawdown = maxdrawdown_5y_pct ? 10 - Math.max(0, Math.min(10, Math.abs(maxdrawdown_5y_pct))) : null;
    const cyclicality = (() => {
      const tag = sector_cyc_tag?.toLowerCase();
      switch (tag) {
        case 'defensive': return 8;
        case 'secular': return 7;
        case 'growth': return 6;
        case 'cyclical': return 4;
        case 'deep-cyclical': return 3;
        default: return 6;
      }
    })();

    return [
      { name: "Base Risk", raw: riskdownside_score_0_10 || "N/A", score: base_risk, weight: "16%" },
      { name: "Net Debt/EBITDA", raw: netdebt_to_ebitda || "N/A", score: net_debt_ebitda, weight: "16%" },
      { name: "Net Cash/MCap", raw: netcash_to_mktcap_pct ? `${netcash_to_mktcap_pct.toFixed(1)}%` : "N/A", score: net_cash_mcap, weight: "16%" },
      { name: "Beta", raw: beta_5y || "N/A", score: beta, weight: "16%" },
      { name: "Max Drawdown", raw: maxdrawdown_5y_pct ? `${maxdrawdown_5y_pct.toFixed(1)}%` : "N/A", score: max_drawdown, weight: "16%" },
      { name: "Cyclicality", raw: sector_cyc_tag || "N/A", score: cyclicality, weight: "16%" }
    ];
  };

  const getSimpleTrace = (categoryName, m) => {
    switch (categoryName) {
      case 'macro':
        return [{ name: "Macro Fit Score", raw: nz(m.macrofit_score_0_10) || "N/A", score: nz(m.macrofit_score_0_10), weight: "100%" }];
      case 'narrative':
        return [{ name: "Narrative Score", raw: nz(m.narrative_score_0_10) || "N/A", score: nz(m.narrative_score_0_10), weight: "100%" }];
      case 'dilution':
        const sharecount_change_5y_pct = toPoints(m.sharecount_change_5y_pct);
        const sbc_to_sales_pct = toPoints(m.sbc_to_sales_pct);
        const dilution = (sharecount_change_5y_pct != null && sbc_to_sales_pct != null) ? 
          Math.max(0, Math.min(10, 10 + 2 * (sharecount_change_5y_pct - sbc_to_sales_pct))) : null;
        return [{ name: "Dilution Risk", raw: `${sharecount_change_5y_pct || 0}% - ${sbc_to_sales_pct || 0}%`, score: dilution, weight: "100%" }];
      default:
        return [];
    }
  };

  let traceData = [];
  switch (categoryName) {
    case 'valuation':
      traceData = getValuationTrace(metrics);
      break;
    case 'quality':
      traceData = getQualityTrace(metrics);
      break;
    case 'capitalAllocation':
      traceData = getCapitalAllocationTrace(metrics);
      break;
    case 'growth':
      traceData = getGrowthTrace(metrics);
      break;
    case 'moat':
      traceData = getMoatTrace(metrics);
      break;
    case 'risk':
      traceData = getRiskTrace(metrics);
      break;
    default:
      traceData = getSimpleTrace(categoryName, metrics);
  }

  return (
    <Card className="mb-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex justify-between items-center">
          <span className="capitalize">{categoryName.replace(/([A-Z])/g, ' $1')}</span>
          <div className="flex items-center gap-2">
            <Badge variant="outline">{categoryScore?.toFixed(2) ?? 'N/A'}</Badge>
            <Badge className="text-xs">{lens[categoryName] || 0}% weight</Badge>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Metric</TableHead>
              <TableHead>Raw Value</TableHead>
              <TableHead>Score (0-10)</TableHead>
              <TableHead>Weight</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {traceData.map((item, idx) => (
              <TableRow key={idx}>
                <TableCell className="font-medium">{item.name}</TableCell>
                <TableCell className="font-mono">{item.raw}</TableCell>
                <TableCell className="font-mono">{item.score?.toFixed(2) ?? 'N/A'}</TableCell>
                <TableCell>{item.weight}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
};

export default function ScoreTraceDrawer({ ticker, metrics, lens, isOpen, onClose }) {
  if (!ticker || !metrics || !lens) return null;

  const scores = computeCategoryScores(metrics);
  const finalScore = computeFinalScore(scores, lens);

  // Log to console for debugging
  try {
    console.table({
      ticker: ticker.symbol,
      lens: lens.name,
      ...scores,
      finalScore: finalScore
    });
  } catch (e) {
    // Ignore console errors in production
  }

  const categories = [
    'valuation', 'quality', 'capitalAllocation', 'growth', 
    'moat', 'risk', 'macro', 'narrative', 'dilution'
  ];

  return (
    <Sheet open={isOpen} onOpenChange={onClose}>
      <SheetContent className="w-[800px] sm:w-[900px] overflow-y-auto">
        <SheetHeader>
          <SheetTitle>Score Trace: {ticker.name} ({ticker.symbol})</SheetTitle>
          <div className="flex items-center gap-4 text-sm text-slate-600">
            <span>Lens: <strong>{lens.name}</strong></span>
            <span>Final Score: <strong className="text-lg text-slate-900">{finalScore.toFixed(2)}</strong></span>
          </div>
        </SheetHeader>

        <div className="py-6 space-y-4">
          {categories.map(categoryName => (
            <CategoryTraceCard
              key={categoryName}
              categoryName={categoryName}
              metrics={metrics}
              categoryScore={scores[categoryName]}
              lens={lens}
            />
          ))}

          <Card className="bg-slate-50">
            <CardHeader>
              <CardTitle>Final Weighted Score Calculation</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-sm space-y-2">
                {categories.map(categoryName => {
                  const weight = lens[categoryName] || 0;
                  const score = scores[categoryName] || 0;
                  const weighted = (score * weight) / 100;
                  return (
                    <div key={categoryName} className="flex justify-between items-center">
                      <span className="capitalize">{categoryName.replace(/([A-Z])/g, ' $1')}</span>
                      <span className="font-mono">{score.toFixed(2)} × {weight}% = {weighted.toFixed(2)}</span>
                    </div>
                  );
                })}
                <div className="border-t pt-2 flex justify-between items-center font-bold">
                  <span>Total</span>
                  <span className="font-mono text-lg">{finalScore.toFixed(2)}</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </SheetContent>
    </Sheet>
  );
}