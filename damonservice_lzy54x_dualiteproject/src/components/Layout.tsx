import React, { useState } from 'react';
import { useStore } from '../services/api';
import { NavLink, Outlet, useNavigate, useLocation } from 'react-router-dom';
import { 
  LayoutDashboard, 
  FolderKanban, 
  Map, 
  LogOut, 
  Menu,
  X,
  Loader2,
  Users,
  Tags,
  Cpu,
  Settings,
  FileText
} from 'lucide-react';
import clsx from 'clsx';

export default function Layout() {
  const { currentUser, logout, fetchInitialData, isLoading } = useStore();
  const navigate = useNavigate();
  const location = useLocation();
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);

  React.useEffect(() => {
    if (!currentUser) {
      navigate('/login');
    } else {
      fetchInitialData();
    }
  }, [currentUser, navigate]);

  React.useEffect(() => {
    setIsSidebarOpen(false);
  }, [location.pathname]);

  if (!currentUser) return null;

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const isAdmin = currentUser.role === 'admin';

  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden" dir="rtl">
      {isSidebarOpen && (
        <div 
          className="fixed inset-0 bg-black/50 z-30 lg:hidden"
          onClick={() => setIsSidebarOpen(false)}
        />
      )}

      <aside 
        className={clsx(
          "fixed inset-y-0 right-0 z-40 w-64 bg-slate-900 text-white flex flex-col shadow-xl transition-transform duration-300 ease-in-out lg:static lg:translate-x-0",
          isSidebarOpen ? "translate-x-0" : "translate-x-full lg:translate-x-0"
        )}
      >
        <div className="p-6 border-b border-slate-700 flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-sky-400">دامون سرویس</h1>
            <p className="text-xs text-slate-400 mt-1">پنل مدیریت (Admin)</p>
          </div>
          <button 
            onClick={() => setIsSidebarOpen(false)}
            className="lg:hidden text-slate-400 hover:text-white"
          >
            <X size={24} />
          </button>
        </div>

        <div className="p-4 border-b border-slate-800 bg-slate-800/50">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-sky-600 flex items-center justify-center text-lg font-bold shrink-0">
              {currentUser.full_name?.charAt(0) || 'A'}
            </div>
            <div className="overflow-hidden">
              <p className="text-sm font-medium truncate">{currentUser.full_name || 'مدیر سیستم'}</p>
              <p className="text-xs text-slate-400 truncate">{currentUser.username}</p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-4 space-y-2 overflow-y-auto custom-scrollbar">
          <div className="text-xs font-bold text-slate-500 px-4 mb-2 mt-2">منوی اصلی</div>
          <NavItem to="/" icon={<LayoutDashboard size={20} />} label="داشبورد" />
          <NavItem to="/projects" icon={<FolderKanban size={20} />} label="پروژه‌ها" />
          <NavItem to="/map" icon={<Map size={20} />} label="نقشه پروژه‌ها" />

          {isAdmin && (
            <>
              <div className="text-xs font-bold text-slate-500 px-4 mb-2 mt-6">مدیریت سیستم</div>
              <NavItem to="/admin/users" icon={<Users size={20} />} label="کاربران" />
              <NavItem to="/admin/categories" icon={<Tags size={20} />} label="دسته‌بندی‌ها" />
              <NavItem to="/admin/devices" icon={<Cpu size={20} />} label="دستگاه‌ها" />
              <NavItem to="/admin/settings" icon={<Settings size={20} />} label="تنظیمات قیمت" />
              <NavItem to="/admin/audit" icon={<FileText size={20} />} label="گزارش عملکرد" />
            </>
          )}
        </nav>

        <div className="p-4 border-t border-slate-800">
          <button 
            onClick={handleLogout}
            className="flex items-center gap-3 w-full px-4 py-2 text-red-400 hover:bg-red-950/30 rounded-lg transition-colors text-sm"
          >
            <LogOut size={18} />
            <span>خروج از حساب</span>
          </button>
        </div>
      </aside>

      <main className="flex-1 flex flex-col overflow-hidden relative w-full">
        <header className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-4 md:px-8 shadow-sm z-10 shrink-0">
          <div className="flex items-center gap-3">
            <button 
              onClick={() => setIsSidebarOpen(true)}
              className="lg:hidden p-2 -mr-2 text-gray-600 hover:bg-gray-100 rounded-lg"
            >
              <Menu size={24} />
            </button>
            <h2 className="text-lg font-semibold text-gray-700 flex items-center gap-2">
              پنل مدیریت
              {isLoading && <Loader2 size={16} className="animate-spin text-sky-600" />}
            </h2>
          </div>
        </header>
        
        <div className="flex-1 overflow-y-auto p-4 md:p-6 lg:p-8 relative">
           <Outlet />
        </div>
      </main>
    </div>
  );
}

function NavItem({ to, icon, label }: { to: string, icon: React.ReactNode, label: string }) {
  return (
    <NavLink 
      to={to} 
      className={({ isActive }) => clsx(
        "flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200",
        isActive 
          ? "bg-sky-600 text-white shadow-lg shadow-sky-900/20" 
          : "text-slate-300 hover:bg-slate-800 hover:text-white"
      )}
    >
      {icon}
      <span className="text-sm font-medium">{label}</span>
    </NavLink>
  );
}
