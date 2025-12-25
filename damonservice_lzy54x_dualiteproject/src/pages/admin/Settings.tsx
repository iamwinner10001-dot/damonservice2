import { useEffect, useState } from 'react';
import { api, Settings } from '../../services/api';
import { Save, Loader2 } from 'lucide-react';

export default function AdminSettings() {
  const [settings, setSettings] = useState<Settings>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      const res = await api.getSettings();
      if (res.ok) setSettings(res.data);
      setLoading(false);
    };
    load();
  }, []);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    const res = await api.updateSettings(settings);
    if (res.ok) alert('تنظیمات با موفقیت ذخیره شد');
    else alert('خطا در ذخیره تنظیمات');
    setSaving(false);
  };

  const handleChange = (key: keyof Settings, val: string) => {
    setSettings(prev => ({ ...prev, [key]: Number(val) }));
  };

  if (loading) return <div className="p-8 text-center">در حال بارگذاری...</div>;

  return (
    <div className="max-w-2xl mx-auto space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-800">تنظیمات قیمت‌گذاری</h1>
      </div>

      <form onSubmit={handleSave} className="bg-white p-6 rounded-xl shadow-sm border border-gray-200 space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <SettingInput label="ضریب تخفیف (Discount Multiplier)" value={settings.discount_multiplier} onChange={v => handleChange('discount_multiplier', v)} />
          <SettingInput label="نرخ حمل به ازای متر (€)" value={settings.freight_rate_per_meter_eur} onChange={v => handleChange('freight_rate_per_meter_eur', v)} />
          <SettingInput label="نرخ گمرک (صورت کسر)" value={settings.customs_numerator} onChange={v => handleChange('customs_numerator', v)} />
          <SettingInput label="نرخ گمرک (مخرج کسر)" value={settings.customs_denominator} onChange={v => handleChange('customs_denominator', v)} />
          <SettingInput label="نرخ گارانتی (Warranty Rate)" value={settings.warranty_rate} onChange={v => handleChange('warranty_rate', v)} />
          <SettingInput label="ضریب کمیسیون (Commission)" value={settings.commission_factor} onChange={v => handleChange('commission_factor', v)} />
          <SettingInput label="ضریب دفتر (Office Factor)" value={settings.office_factor} onChange={v => handleChange('office_factor', v)} />
          <SettingInput label="ضریب سود (Profit Factor)" value={settings.profit_factor} onChange={v => handleChange('profit_factor', v)} />
        </div>

        <div className="pt-4 border-t border-gray-100">
          <button 
            type="submit" 
            disabled={saving}
            className="w-full py-3 bg-sky-600 text-white rounded-lg hover:bg-sky-700 flex items-center justify-center gap-2 font-medium disabled:opacity-50"
          >
            {saving ? <Loader2 className="animate-spin" /> : <Save size={20} />}
            ذخیره تنظیمات
          </button>
        </div>
      </form>
    </div>
  );
}

function SettingInput({ label, value, onChange }: any) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <input 
        type="number" 
        step="0.01"
        value={value || ''}
        onChange={e => onChange(e.target.value)}
        className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none font-mono text-left"
        dir="ltr"
      />
    </div>
  );
}
