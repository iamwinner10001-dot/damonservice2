import { useEffect, useState } from 'react';
import { api, Category } from '../../services/api';
import { Trash2, Plus, Tag, Edit2, CheckCircle, XCircle, Loader2 } from 'lucide-react';

export default function AdminCategories() {
  const [categories, setCategories] = useState<Category[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  
  // Form State
  const [formData, setFormData] = useState<Partial<Category>>({ 
    category_name: '', 
    description: '',
    is_active: true
  });
  const [editingId, setEditingId] = useState<string | null>(null);

  const loadData = async () => {
    setLoading(true);
    const res = await api.getAdminCategories();
    if (res.ok) setCategories(res.data);
    setLoading(false);
  };

  useEffect(() => { loadData(); }, []);

  const handleOpenModal = (cat?: Category) => {
    if (cat) {
      setEditingId(cat.id);
      setFormData({ 
        category_name: cat.category_name, 
        description: cat.description || '',
        is_active: String(cat.is_active) === 'true' || cat.is_active === true
      });
    } else {
      setEditingId(null);
      setFormData({ category_name: '', description: '', is_active: true });
    }
    setShowModal(true);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.category_name) return alert('نام دسته‌بندی الزامی است');
    
    setSubmitting(true);
    let res;
    
    if (editingId) {
      res = await api.updateCategory({ ...formData, id: editingId });
    } else {
      res = await api.createCategory(formData);
    }
    
    setSubmitting(false);
    
    if (res.ok) {
      setShowModal(false);
      loadData();
    } else {
      alert(res.message || 'خطا در ذخیره اطلاعات');
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('آیا از حذف این دسته‌بندی اطمینان دارید؟')) return;
    const res = await api.deleteCategory(id);
    if (res.ok) loadData();
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-800">مدیریت دسته‌بندی‌ها</h1>
        <button 
          onClick={() => handleOpenModal()}
          className="bg-sky-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-sky-700 transition-colors"
        >
          <Plus size={18} />
          <span>دسته‌بندی جدید</span>
        </button>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <table className="w-full text-right">
          <thead className="bg-gray-50 text-gray-500 text-sm">
            <tr>
              <th className="p-4 font-medium">نام دسته‌بندی</th>
              <th className="p-4 font-medium">توضیحات</th>
              <th className="p-4 font-medium">وضعیت</th>
              <th className="p-4 font-medium">عملیات</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr><td colSpan={4} className="p-8 text-center text-gray-400">در حال بارگذاری...</td></tr>
            ) : categories.map((cat) => {
              const isActive = String(cat.is_active) === 'true' || cat.is_active === true;
              return (
                <tr key={cat.id} className="hover:bg-gray-50 transition-colors">
                  <td className="p-4 font-medium text-gray-800 flex items-center gap-2">
                    <div className="w-8 h-8 bg-purple-50 text-purple-600 rounded-lg flex items-center justify-center shrink-0">
                      <Tag size={16} />
                    </div>
                    {cat.category_name}
                  </td>
                  <td className="p-4 text-gray-500 text-sm max-w-xs truncate">
                    {cat.description || '-'}
                  </td>
                  <td className="p-4">
                    <span className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      isActive ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                    }`}>
                      {isActive ? <CheckCircle size={12} /> : <XCircle size={12} />}
                      {isActive ? 'فعال' : 'غیرفعال'}
                    </span>
                  </td>
                  <td className="p-4 flex gap-2">
                    <button onClick={() => handleOpenModal(cat)} className="text-blue-400 hover:text-blue-600 p-1">
                      <Edit2 size={18} />
                    </button>
                    <button onClick={() => handleDelete(cat.id)} className="text-red-400 hover:text-red-600 p-1">
                      <Trash2 size={18} />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {showModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-md p-6">
            <h2 className="text-xl font-bold mb-4">{editingId ? 'ویرایش دسته‌بندی' : 'افزودن دسته‌بندی'}</h2>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">نام دسته‌بندی</label>
                <input 
                  type="text" 
                  value={formData.category_name}
                  onChange={e => setFormData({...formData, category_name: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                  placeholder="مثال: سیستم‌های VRF"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">توضیحات</label>
                <textarea 
                  value={formData.description}
                  onChange={e => setFormData({...formData, description: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none h-24 resize-none"
                  placeholder="توضیحات اختیاری..."
                />
              </div>
              
              <div className="flex items-center gap-2">
                <input 
                  type="checkbox" 
                  id="is_active"
                  checked={Boolean(formData.is_active)}
                  onChange={e => setFormData({...formData, is_active: e.target.checked})}
                  className="w-4 h-4 text-sky-600 rounded focus:ring-sky-500"
                />
                <label htmlFor="is_active" className="text-sm text-gray-700">وضعیت فعال</label>
              </div>

              <div className="flex gap-3 mt-6">
                <button type="button" onClick={() => setShowModal(false)} className="flex-1 py-2 text-gray-600 hover:bg-gray-100 rounded-lg">انصراف</button>
                <button 
                  type="submit" 
                  disabled={submitting}
                  className="flex-1 py-2 bg-sky-600 text-white rounded-lg hover:bg-sky-700 flex items-center justify-center gap-2"
                >
                  {submitting && <Loader2 size={16} className="animate-spin" />}
                  ذخیره
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
