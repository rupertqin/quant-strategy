export default function LoadingSpinner({ text = '加载中...', className = '' }) {
  return (
    <div className={`flex flex-col items-center justify-center py-16 space-y-4 ${className}`}>
      <div className="relative w-10 h-10">
        <div className="absolute inset-0 rounded-full border-[3px] border-slate-200 border-t-indigo-500 animate-spin" />
        <div
          className="absolute inset-1.5 rounded-full border-[3px] border-slate-100 border-b-sky-400 animate-spin"
          style={{ animationDirection: 'reverse', animationDuration: '1.2s' }}
        />
      </div>
      <p className="text-slate-400 text-sm font-medium tracking-wide">{text}</p>
    </div>
  );
}
