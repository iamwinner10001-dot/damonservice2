import { useEffect, useState } from 'react';
import { api, User } from '../../services/api';
import { Trash2, UserPlus, Shield, User as UserIcon, Loader2 } from 'lucide-react';

export default function AdminUsers() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [formData, setFormData] = useState({ username: '', password: '', full_name: '', role: 'employee' });

  const loadUsers = async () => {
    setLoading(true);
    const res = await api.getUsers();
    if (res.ok) setUsers(res.data);
    setLoading(false);
  };

  useEffect(() => { loadUsers(); }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.username || !formData.password || !formData.full_name) return alert('لطفا تمام فیلدها را پر کنید');
    
    setSubmitting(true);
    // Backend handles salt/hash generation
    const res = await api.createUser(formData);
    setSubmitting(false);
    
    if (res.ok) {
      setShowModal(false);
      setFormData({ username: '', password: '', full_name: '', role: 'employee' });
      loadUsers();
    } else {
      alert(res.message || 'خطا در ایجاد کاربر');
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('آیا از حذف این کاربر اطمینان دارید؟')) return;
    const res = await api.deleteUser(id);
    if (res.ok) loadUsers();
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-800">مدیریت کاربران</h1>
        <button 
          onClick={() => setShowModal(true)}
          className="bg-sky-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-sky-700 transition-colors"
        >
          <UserPlus size={18} />
          <span>افزودن کاربر</span>
        </button>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <table className="w-full text-right">
          <thead className="bg-gray-50 text-gray-500 text-sm">
            <tr>
              <th className="p-4 font-medium">نام کامل</th>
              <th className="p-4 font-medium">نام کاربری</th>
              <th className="p-4 font-medium">نقش</th>
              <th className="p-4 font-medium">عملیات</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr><td colSpan={4} className="p-8 text-center text-gray-400">در حال بارگذاری...</td></tr>
            ) : users.map((user) => (
              <tr key={user.id} className="hover:bg-gray-50 transition-colors">
                <td className="p-4 font-medium text-gray-800">{user.full_name || '-'}</td>
                <td className="p-4 text-gray-600 font-mono text-sm">{user.username}</td>
                <td className="p-4">
                  <span className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    user.role === 'admin' ? 'bg-purple-100 text-purple-700' :
                    user.role === 'sales_manager' ? 'bg-blue-100 text-blue-700' :
                    'bg-gray-100 text-gray-700'
                  }`}>
                    {user.role === 'admin' ? <Shield size={12} /> : <UserIcon size={12} />}
                    {user.role === 'admin' ? 'مدیر سیستم' : user.role === 'sales_manager' ? 'مدیر فروش' : 'کارمند'}
                  </span>
                </td>
                <td className="p-4">
                  {user.username !== 'admin' && (
                    <button onClick={() => handleDelete(user.id!)} className="text-red-400 hover:text-red-600">
                      <Trash2 size={18} />
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-md p-6">
            <h2 className="text-xl font-bold mb-4">افزودن کاربر جدید</h2>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">نام کامل</label>
                <input 
                  type="text" 
                  value={formData.full_name}
                  onChange={e => setFormData({...formData, full_name: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                  placeholder="مثال: علی محمدی"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">نام کاربری</label>
                <input 
                  type="text" 
                  value={formData.username}
                  onChange={e => setFormData({...formData, username: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                  dir="ltr"
                  placeholder="user123"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">رمز عبور</label>
                <input 
                  type="text" 
                  value={formData.password}
                  onChange={e => setFormData({...formData, password: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                  dir="ltr"
                  placeholder="******"
                />
                <p className="text-xs text-gray-400 mt-1">رمز عبور به صورت Hash ذخیره می‌شود.</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">نقش کاربری</label>
                <select 
                  value={formData.role}
                  onChange={e => setFormData({...formData, role: e.target.value})}
                  className="w-full p-2 border rounded-lg focus:ring-2 focus:ring-sky-500 outline-none"
                >
                  <option value="employee">کارمند (Employee)</option>
                  <option value="sales_manager">مدیر فروش (Sales Manager)</option>
                  <option value="admin">مدیر سیستم (Admin)</option>
                </select>
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
