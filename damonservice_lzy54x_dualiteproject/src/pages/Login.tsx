import React, { useEffect } from 'react';
import { useStore } from '../services/api';
import { useNavigate } from 'react-router-dom';

export default function Login() {
  const { login, currentUser, isLoading } = useStore();
  const navigate = useNavigate();

  useEffect(() => {
    if (currentUser) navigate('/');
  }, [currentUser, navigate]);

  return (
    <div className="flex items-center justify-center min-h-screen bg-gray-100" dir="rtl">
      <div className="bg-white p-8 rounded-xl shadow-lg text-center max-w-md w-full border border-gray-200">
        <h1 className="text-2xl font-bold text-gray-800 mb-2">دامون سرویس</h1>
        <p className="text-gray-500 mb-8">پنل مدیریت پروژه (Admin)</p>
        
        <button 
          onClick={() => login()} 
          disabled={isLoading}
          className="w-full py-3 px-4 bg-sky-600 hover:bg-sky-700 text-white rounded-lg font-medium transition-colors disabled:opacity-50 flex items-center justify-center gap-2 shadow-md"
        >
          {isLoading ? 'در حال اتصال...' : 'ورود مدیر'}
        </button>
      </div>
    </div>
  );
}
