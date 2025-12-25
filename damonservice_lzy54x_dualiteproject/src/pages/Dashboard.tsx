import { useEffect, useState } from 'react';
import { api, Project } from '../services/api';
import ConnectionStatus from '../components/ConnectionStatus';
import { FolderKanban, CheckCircle2, XCircle, Clock } from 'lucide-react';

export default function Dashboard() {
  const [stats, setStats] = useState({ total: 0, approved: 0, rejected: 0, pending: 0 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadStats = async () => {
      setLoading(true);
      const res = await api.getProjects();
      if (res.ok && Array.isArray(res.data)) {
        const projects: Project[] = res.data;
        setStats({
          total: projects.length,
          approved: projects.filter(p => p.status === 'approved').length,
          rejected: projects.filter(p => p.status === 'rejected').length,
          pending: projects.filter(p => p.status === 'pending_approval').length,
        });
      }
      setLoading(false);
    };
    loadStats();
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-800">داشبورد مدیریت</h1>
          <p className="text-gray-500 mt-1 text-sm">نمای کلی سیستم و وضعیت پروژه‌ها</p>
        </div>
        <div className="w-full md:w-auto">
          <ConnectionStatus />
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard 
          title="کل پروژه‌ها" 
          value={stats.total} 
          icon={<FolderKanban size={24} />} 
          color="bg-blue-50 text-blue-600" 
          loading={loading}
        />
        <StatCard 
          title="در انتظار تایید" 
          value={stats.pending} 
          icon={<Clock size={24} />} 
          color="bg-yellow-50 text-yellow-600" 
          loading={loading}
        />
        <StatCard 
          title="تایید شده" 
          value={stats.approved} 
          icon={<CheckCircle2 size={24} />} 
          color="bg-green-50 text-green-600" 
          loading={loading}
        />
        <StatCard 
          title="رد شده" 
          value={stats.rejected} 
          icon={<XCircle size={24} />} 
          color="bg-red-50 text-red-600" 
          loading={loading}
        />
      </div>
    </div>
  );
}

function StatCard({ title, value, icon, color, loading }: any) {
  return (
    <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200 flex items-center gap-4">
      <div className={`w-12 h-12 rounded-lg flex items-center justify-center ${color}`}>
        {icon}
      </div>
      <div>
        <p className="text-sm text-gray-500 font-medium">{title}</p>
        {loading ? (
          <div className="h-8 w-16 bg-gray-100 rounded animate-pulse mt-1"></div>
        ) : (
          <h3 className="text-2xl font-bold text-gray-800">{value}</h3>
        )}
      </div>
    </div>
  );
}
