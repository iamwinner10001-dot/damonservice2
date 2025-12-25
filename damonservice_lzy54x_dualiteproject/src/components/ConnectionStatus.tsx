import { useState, useEffect } from 'react';
import { api } from '../services/api';
import { Wifi, WifiOff, RefreshCw } from 'lucide-react';
import clsx from 'clsx';

export default function ConnectionStatus() {
  const [status, setStatus] = useState<'checking' | 'online' | 'offline'>('checking');
  const [lastCheck, setLastCheck] = useState<Date | null>(null);

  const checkConnection = async () => {
    setStatus('checking');
    const res = await api.checkHealth();
    if (res.ok) {
      setStatus('online');
      setLastCheck(new Date());
    } else {
      setStatus('offline');
    }
  };

  useEffect(() => {
    checkConnection();
    const interval = setInterval(checkConnection, 60000); // Check every minute
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-200 flex items-center justify-between">
      <div className="flex items-center gap-3">
        <div className={clsx(
          "w-10 h-10 rounded-full flex items-center justify-center",
          status === 'online' ? "bg-green-100 text-green-600" :
          status === 'offline' ? "bg-red-100 text-red-600" :
          "bg-yellow-100 text-yellow-600"
        )}>
          {status === 'online' ? <Wifi size={20} /> : 
           status === 'offline' ? <WifiOff size={20} /> : 
           <RefreshCw size={20} className="animate-spin" />}
        </div>
        <div>
          <h3 className="font-bold text-gray-800 text-sm">وضعیت اتصال به دیتابیس</h3>
          <p className={clsx("text-xs font-medium", 
            status === 'online' ? "text-green-600" : 
            status === 'offline' ? "text-red-500" : "text-yellow-600"
          )}>
            {status === 'online' ? 'متصل (Google Sheets)' : 
             status === 'offline' ? 'قطع ارتباط' : 'در حال بررسی...'}
          </p>
        </div>
      </div>
      
      <button 
        onClick={checkConnection}
        className="p-2 text-gray-400 hover:text-sky-600 hover:bg-sky-50 rounded-full transition-colors"
        title="بروزرسانی وضعیت"
      >
        <RefreshCw size={18} className={clsx(status === 'checking' && "animate-spin")} />
      </button>
    </div>
  );
}
