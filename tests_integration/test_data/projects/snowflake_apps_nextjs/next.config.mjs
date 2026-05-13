// output: "standalone" is required for Snowflake Apps Deploy (app.yml runs server.js).
// images.unoptimized avoids pulling the sharp native binary in minimal builds.

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
};

export default nextConfig;
