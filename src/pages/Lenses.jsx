import React, { useState, useEffect } from "react";
import { LensPreset } from "@/api/entities";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Slider } from "@/components/ui/slider";
import { 
  Plus, 
  Edit3, 
  Trash2, 
  Save, 
  X, 
  Layers3,
  Target,
  TrendingUp,
  Shield,
  Building2
} from "lucide-react";
import { 
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

const CATEGORY_INFO = {
  valuation: { name: "Valuation", icon: Target, color: "blue", description: "P/E, PEG, FCF yield..." },
  quality: { name: "Quality", icon: Shield, color: "green", description: "ROIC, FCF margins..." },
  capitalAllocation: { name: "Capital Allocation", icon: Building2, color: "purple", description: "Buybacks, debt, ROIIC..." },
  growth: { name: "Growth", icon: TrendingUp, color: "emerald", description: "CAGR, acceleration..." },
  moat: { name: "Moat", icon: Shield, color: "indigo", description: "GM trend, recurring revenue..." },
  risk: { name: "Risk", icon: Shield, color: "red", description: "Beta, drawdowns, debt..." },
  macro: { name: "Macro Fit", icon: Target, color: "amber", description: "Alignment with macro trends" },
  narrative: { name: "Narrative", icon: Target, color: "pink", description: "Market narrative strength" },
  dilution: { name: "Dilution", icon: Target, color: "slate", description: "Share count, SBC..." }
};

export default function Lenses() {
  const [lenses, setLenses] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [editingLens, setEditingLens] = useState(null);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  useEffect(() => {
    loadLenses();
  }, []);

  const loadLenses = async () => {
    try {
      const data = await LensPreset.list();
      setLenses(data);
    } catch (error) {
      console.error("Error loading lenses:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const createDefaultLens = () => ({
    id: '',
    name: '',
    valuation: 20,
    quality: 20,
    capitalAllocation: 10,
    growth: 15,
    moat: 15,
    risk: 10,
    macro: 5,
    narrative: 3,
    dilution: 2
  });

  const handleEdit = (lens) => {
    setEditingLens(lens ? {...lens} : createDefaultLens());
    setIsDialogOpen(true);
  };

  const handleSave = async () => {
    if (!editingLens) return;
    
    const totalWeight = Object.keys(CATEGORY_INFO).reduce((sum, key) => 
      sum + (editingLens[key] || 0), 0);
      
    if (Math.abs(totalWeight - 100) > 0.01) {
      alert("Weights must sum to exactly 100%");
      return;
    }
    
    try {
      const lensDataToSave = {...editingLens};
      delete lensDataToSave.created_date;
      delete lensDataToSave.updated_date;
      delete lensDataToSave.created_by;

      if (editingLens.id) {
        await LensPreset.update(editingLens.id, lensDataToSave);
      } else {
        const newId = editingLens.name.toLowerCase().replace(/[^a-z0-9]/g, '_');
        await LensPreset.create({ ...lensDataToSave, id: newId });
      }
      
      await loadLenses();
      setIsDialogOpen(false);
      setEditingLens(null);
    } catch (error) {
      console.error("Error saving lens:", error);
      alert("Error saving lens preset");
    }
  };

  const handleDelete = async (lensId) => {
    if (!confirm("Are you sure you want to delete this lens preset?")) return;
    
    try {
      await LensPreset.delete(lensId);
      await loadLenses();
    } catch (error) {
      console.error("Error deleting lens:", error);
    }
  };

  const updateWeight = (category, value) => {
    setEditingLens(prev => ({ ...prev, [category]: value[0] }));
  };

  const getTotalWeight = () => {
    if (!editingLens) return 0;
    return Object.keys(CATEGORY_INFO).reduce((sum, key) => 
      sum + (editingLens[key] || 0), 0);
  };

  const normalizeWeights = () => {
    const total = getTotalWeight();
    if (total === 0) return;
    
    const normalized = { ...editingLens };
    Object.keys(CATEGORY_INFO).forEach(key => {
      normalized[key] = Math.round((normalized[key] / total) * 100);
    });
    
    const newTotal = Object.keys(CATEGORY_INFO).reduce((sum, key) => 
      sum + normalized[key], 0);
    if (newTotal !== 100) {
      normalized.valuation += (100 - newTotal);
    }
    
    setEditingLens(normalized);
  };

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-8 bg-slate-200 rounded animate-pulse" />
        <div className="grid gap-4">
          {[1, 2, 3].map(i => (
            <div key={i} className="h-32 bg-slate-200 rounded animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900">Investment Lenses</h1>
          <p className="text-slate-600 mt-1">
            Customize scoring weights to match your investment philosophy
          </p>
        </div>
        <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
          <DialogTrigger asChild>
            <Button onClick={() => handleEdit(null)} className="gap-2 bg-slate-900 hover:bg-slate-800">
              <Plus className="w-4 h-4" />
              Create Lens
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>
                {editingLens?.id ? 'Edit Lens Preset' : 'Create New Lens Preset'}
              </DialogTitle>
            </DialogHeader>
            
            {editingLens && (
              <div className="space-y-6">
                <div>
                  <Label htmlFor="name">Lens Name</Label>
                  <Input
                    id="name"
                    value={editingLens.name}
                    onChange={(e) => setEditingLens(prev => ({ ...prev, name: e.target.value }))}
                    placeholder="e.g., Conservative Value"
                    className="mt-1"
                  />
                </div>

                <div className="space-y-4">
                  <div className="flex justify-between items-center">
                    <h3 className="text-lg font-semibold">Category Weights</h3>
                    <div className="flex items-center gap-4">
                      <Badge variant={getTotalWeight() === 100 ? "default" : "destructive"}>
                        Total: {getTotalWeight()}%
                      </Badge>
                      <Button variant="outline" size="sm" onClick={normalizeWeights}>
                        Normalize to 100%
                      </Button>
                    </div>
                  </div>

                  <div className="grid gap-6">
                    {Object.entries(CATEGORY_INFO).map(([key, info]) => {
                      const Icon = info.icon;
                      return (
                        <div key={key} className="space-y-2">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                              <Icon className={`w-5 h-5 text-${info.color}-600`} />
                              <div>
                                <Label className="text-sm font-medium">{info.name}</Label>
                                <p className="text-xs text-slate-500">{info.description}</p>
                              </div>
                            </div>
                            <Badge variant="outline" className="min-w-[60px] justify-center">
                              {editingLens[key] || 0}%
                            </Badge>
                          </div>
                          <Slider
                            value={[editingLens[key] || 0]}
                            onValueChange={(value) => updateWeight(key, value)}
                            max={50}
                            step={1}
                            className="w-full"
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>

                <div className="flex justify-end gap-3 pt-4 border-t">
                  <Button variant="outline" onClick={() => setIsDialogOpen(false)}>
                    <X className="w-4 h-4 mr-2" />
                    Cancel
                  </Button>
                  <Button 
                    onClick={handleSave}
                    disabled={!editingLens.name || getTotalWeight() !== 100}
                    className="bg-slate-900 hover:bg-slate-800"
                  >
                    <Save className="w-4 h-4 mr-2" />
                    Save Lens
                  </Button>
                </div>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>

      {/* Lens Grid */}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        {lenses.map((lens) => (
          <Card key={lens.id} className="hover:shadow-lg transition-all duration-200">
            <CardHeader className="pb-4">
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Layers3 className="w-5 h-5 text-slate-600" />
                  {lens.name}
                </CardTitle>
                <div className="flex gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleEdit(lens)}
                    className="h-8 w-8"
                  >
                    <Edit3 className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleDelete(lens.id)}
                    className="h-8 w-8 text-red-600 hover:text-red-700"
                  >
                    <Trash2 className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              {Object.entries(CATEGORY_INFO).map(([key, info]) => {
                const weight = lens[key] || 0;
                const Icon = info.icon;
                return (
                  <div key={key} className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Icon className={`w-4 h-4 text-${info.color}-600`} />
                      <span className="text-sm text-slate-600">{info.name}</span>
                    </div>
                    <Badge variant="outline" className="text-xs">
                      {weight}%
                    </Badge>
                  </div>
                );
              })}
            </CardContent>
          </Card>
        ))}
      </div>

      {lenses.length === 0 && (
        <Card className="p-8 text-center">
          <Layers3 className="w-12 h-12 mx-auto mb-4 text-slate-300" />
          <h3 className="text-lg font-medium text-slate-900 mb-2">No Lens Presets</h3>
          <p className="text-slate-600 mb-4">
            Create your first investment lens to start analyzing stocks with your custom weighting
          </p>
          <Button onClick={() => handleEdit(null)} className="gap-2">
            <Plus className="w-4 h-4" />
            Create Your First Lens
          </Button>
        </Card>
      )}
    </div>
  );
}