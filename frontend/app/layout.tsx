import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Raft Visualizer",
  description: "Real-time Raft consensus algorithm visualizer",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-slate-900 text-white antialiased">{children}</body>
    </html>
  );
}
