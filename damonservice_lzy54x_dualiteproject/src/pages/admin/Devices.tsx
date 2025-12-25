import { useEffect, useState } from 'react';
import { api, Device, Category, Settings } from '../../services/api';
import { Trash2, Plus, Search, Cpu, Calculator, X } from 'lucide-react';

export default function AdminDevices() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  
  // Modal States
  const [showModal, setShowModal] = useState(false);
  const [showCalcModal, setShowCalcModal] = useState(false);
  const [selectedDeviceForCalc, setSelectedDeviceForCalc] = useState<Device | null>(null);

  const [formData, setFormData] = useState({ 
    model_name: '', 
    category_id: '', 
    factory_pricelist_eur: 0,
    length_meter: 0,
    weight_unit: 0
  });

  const loadData = async () => {
    setLoading(true);
    const [dRes, cRes, sRes] = await Promise.all([
      api.getDevices(), 
      api.getCategories(),
      api.getSettings()
    ]);
    if (dRes.ok) setDevices(dRes.data);
    if (cRes.ok) setCategories(cRes.data);
    if (sRes.ok) setSettings(sRes.data);
    setLoading(false);
  };

  useEffect(() => { loadData(); }, []);

  const filtered = devices.filter(d => d.model_name.toLowerCase().includes(search.toLowerCase()));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.model_name || !formData.category_id) return alert('مدل و دسته‌بندی الزامی است');
    
    const res = await api.createDevice(formData);
    if (res.ok) {
      setShowModal(false);
      setFormData({ model_name: '', category_id: '', factory_pricelist_eur: 0, length_meter: 0, weight_unit: 0 });
      loadData();
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('حذف شود؟')) return;
    const res = await api.deleteDevice(id);
    if (res.ok) loadData();
  };

  const handleCalcClick = (device: Device) => {
    setSelectedDeviceForCalc(device);
    setShowCalcModal(true);
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row justify-between items-center gap-4">
        <h1 className="text-2xl font-bold text-gray-800">مدیریت دستگاه‌ها</h1>
        <button 
          onClick={() => setShowModal(true)}
          className="bg-sky-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-sky-700 w-full md:w-auto justify-center"
        >
          <Plus size={18} />
          <span>دستگاه جدید</span>
        </button>
      </div>

      <div className="bg-white p-4 rounded-xl border border-gray-200 flex items-center gap-3">
        <Search className="text-gray-400" size={20} />
        <input 
          type="text" 
          placeholder="جستجو در نام مدل..." 
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="flex-1 outline-none text-gray-700"
        />
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <table className="w-full text-right">
          <thead className="bg-gray-50 text-gray-500 text-sm">
            <tr>
              <th className="p-4 font-medium">مدل دستگاه</th>
              <th className="p-4 font-medium">دسته‌بندی</th>
              <th className="p-4 font-medium">قیمت پایه (€)</th>
              <th className="p-4 font-medium">طول (m)</th>
              <th className="p-4 font-medium">وزن (kg)</th>
              <th className="p-4 font-medium text-center">عملیات</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr><td colSpan={6} className="p-8 text-center text-gray-400">در حال بارگذاری...</td></tr>
            ) : filtered.map((dev) => (
              <tr key={dev.id} className="hover:bg-gray-50 transition-colors">
                <td className="p-4 font-medium text-gray-800 flex items-center gap-2">
                  <Cpu size={16} className="text-gray-400" />
                  {dev.model_name}
                </td>
                <td className="p-4 text-gray-600 text-sm">
                  {categories.find(c => String(c.id) === String(dev.category_id))?.category_name || '-'}
                </td>
                <td className="p-4 font-mono text-sm">
                  {dev.factory_pricelist_eur ? `€ ${dev.factory_pricelist_eur.toLocaleString()}` : '-'}
                </td>
                <td className="p-4 font-mono text-sm text-gray-600">{dev.length_meter || 0}</td>
                <td className="p-4 font-mono text-sm text-gray-600">{dev.weight_unit || 0}</td>
                <td className="p-4 flex gap-2 justify-center">
                  <button 
                    onClick={() => handleCalcClick(dev)} 
                    className="text-sky-500 hover:text-sky-700 p-1"
                    title="محاسبه قیمت"
                  >
                    <Calculator size={18} />
                  </button>
                  <button onClick={() => handleDelete(dev.id)} className="text-red-400 hover:text-red-600 p-1">
                    <Trash2 size={18} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* CREATE MODAL */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-md p-6 max-h-[90vh] overflow-y-auto">
            <h2 className="text-xl font-bold mb-4">افزودن دستگاه جدید</h2>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">نام مدل</label>
                <input 
                  type="text" 
                  value={formData.model_name}
                  onChange={e => setFormData({...formData, model_name: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">دسته‌بندی</label>
                <select 
                  value={formData.category_id}
                  onChange={e => setFormData({...formData, category_id: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                >
                  <option value="">انتخاب کنید...</option>
                  {categories.map(c => <option key={c.id} value={c.id}>{c.category_name}</option>)}
                </select>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">قیمت پایه (€)</label>
                  <input 
                    type="number" 
                    value={formData.factory_pricelist_eur}
                    onChange={e => setFormData({...formData, factory_pricelist_eur: Number(e.target.value)})}
                    className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                    dir="ltr"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">طول (متر)</label>
                  <input 
                    type="number" 
                    step="0.01"
                    value={formData.length_meter}
                    onChange={e => setFormData({...formData, length_meter: Number(e.target.value)})}
                    className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                    dir="ltr"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">وزن (کیلوگرم)</label>
                  <input 
                    type="number" 
                    step="0.01"
                    value={formData.weight_unit}
                    onChange={e => setFormData({...formData, weight_unit: Number(e.target.value)})}
                    className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                    dir="ltr"
                  />
                </div>
              </div>
              <div className="flex gap-3 mt-6">
                <button type="button" onClick={() => setShowModal(false)} className="flex-1 py-2 text-gray-600 hover:bg-gray-100 rounded-lg">انصراف</button>
                <button type="submit" className="flex-1 py-2 bg-sky-600 text-white rounded-lg hover:bg-sky-700">ذخیره</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* CALCULATION MODAL */}
      {showCalcModal && selectedDeviceForCalc && settings && (
        <CalculationModal 
          device={selectedDeviceForCalc} 
          settings={settings} 
          onClose={() => setShowCalcModal(false)} 
        />
      )}
    </div>
  );
}

function CalculationModal({ device, settings, onClose }: { device: Device, settings: Settings, onClose: () => void }) {
  const P = Number(device.factory_pricelist_eur || 0);
  const L = Number(device.length_meter || 0);
  const W = Number(device.weight_unit || 0);

  // Defaults per "Final Correct Version"
  const D   = settings.discount_multiplier !== undefined ? Number(settings.discount_multiplier) : 0.38;
  const F   = settings.freight_rate_per_meter_eur !== undefined ? Number(settings.freight_rate_per_meter_eur) : 1000;
  const CN  = settings.customs_numerator !== undefined ? Number(settings.customs_numerator) : 350000;
  const CD  = settings.customs_denominator !== undefined ? Number(settings.customs_denominator) : 150000;
  const WR  = settings.warranty_rate !== undefined ? Number(settings.warranty_rate) : 0.05;
  const COM = settings.commission_factor !== undefined ? Number(settings.commission_factor) : 0.95;
  const OFF = settings.office_factor !== undefined ? Number(settings.office_factor) : 0.95;
  const PF  = settings.profit_factor !== undefined ? Number(settings.profit_factor) : 0.65;

  const companyPrice = P * D;
  const shipment = L * F;
  const custom = W * (CN / (CD || 1));
  const warranty = companyPrice * WR;
  const subtotal = companyPrice + shipment + custom + warranty;
  const afterCommission = subtotal / (COM || 1);
  const afterOffice = afterCommission / (OFF || 1);
  const sellPrice = afterOffice / (PF || 1);

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-lg p-6 relative">
        <button onClick={onClose} className="absolute top-4 left-4 text-gray-400 hover:text-gray-600">
          <X size={24} />
        </button>
        
        <h2 className="text-xl font-bold mb-1 text-gray-800">محاسبه قیمت</h2>
        <p className="text-sm text-gray-500 mb-6">{device.model_name}</p>

        <div className="space-y-3 text-sm">
          <Row label="قیمت پایه (P)" value={P} unit="€" />
          <Row label="قیمت شرکت (P × D)" value={companyPrice} unit="€" sub={`D = ${D}`} />
          <Row label="هزینه حمل (L × F)" value={shipment} unit="€" sub={`L=${L}, F=${F}`} />
          <Row label="هزینه گمرک (W × CN/CD)" value={custom} unit="€" sub={`W=${W}, Ratio=${(CN/(CD||1)).toFixed(2)}`} />
          <Row label="گارانتی (Company × WR)" value={warranty} unit="€" sub={`WR = ${WR}`} />
          
          <div className="border-t border-gray-200 my-2"></div>
          
          <Row label="جمع کل (Subtotal)" value={subtotal} unit="€" bold />
          <Row label="بعد از کمیسیون (/ COM)" value={afterCommission} unit="€" sub={`COM = ${COM}`} />
          <Row label="بعد از دفتر (/ OFF)" value={afterOffice} unit="€" sub={`OFF = ${OFF}`} />
          
          <div className="bg-sky-50 p-3 rounded-lg mt-4 border border-sky-100">
            <div className="flex justify-between items-center text-sky-800">
              <span className="font-bold">قیمت نهایی فروش (/ PF)</span>
              <span className="font-bold text-lg">{sellPrice.toLocaleString(undefined, { maximumFractionDigits: 2 })} €</span>
            </div>
            <div className="text-xs text-sky-600 mt-1 text-left" dir="ltr">PF = {PF}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Row({ label, value, unit, sub, bold }: any) {
  return (
    <div className={`flex justify-between items-center ${bold ? 'font-bold text-gray-900' : 'text-gray-700'}`}>
      <div className="flex flex-col">
        <span>{label}</span>
        {sub && <span className="text-[10px] text-gray-400">{sub}</span>}
      </div>
      <span className="font-mono">{value.toLocaleString(undefined, { maximumFractionDigits: 2 })} {unit}</span>
    </div>
  );
}
