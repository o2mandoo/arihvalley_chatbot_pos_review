#!/usr/bin/env node
require("dotenv/config");
const { PrismaClient } = require("@prisma/client");
const { PrismaPg } = require("@prisma/adapter-pg");
const { Pool } = require("pg");

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is required.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

const BRANCH_NAME = process.env.SEED_BRANCH_NAME || "왕십리한양대점";
const BRANCH_AREA_TYPE = process.env.SEED_BRANCH_AREA_TYPE || "대학가";

const STAFF_SEED = [
  {
    name: "김민수",
    role: "매니저",
    color: "#3B82F6",
    phone: "010-1234-5678",
    hourlyWage: 12000,
    fixedDays: ["월", "화", "수", "목", "금"],
  },
  {
    name: "이지현",
    role: "홀서빙",
    color: "#8B5CF6",
    phone: "010-2345-6789",
    hourlyWage: 10000,
    fixedDays: ["화", "수", "목", "금", "토"],
  },
  {
    name: "박준호",
    role: "홀서빙",
    color: "#EC4899",
    phone: "010-3456-7890",
    hourlyWage: 10000,
    fixedDays: ["월", "수", "금", "토", "일"],
  },
  {
    name: "최수진",
    role: "주방",
    color: "#F59E0B",
    phone: "010-4567-8901",
    hourlyWage: 11000,
    fixedDays: ["월", "화", "수", "목", "금"],
  },
  {
    name: "정태웅",
    role: "주방",
    color: "#10B981",
    phone: "010-5678-9012",
    hourlyWage: 11000,
    fixedDays: ["수", "목", "금", "토", "일"],
  },
];

const DAY_NAMES = ["일", "월", "화", "수", "목", "금", "토"];

function getShift(role, dayOffset) {
  if (role === "매니저") {
    return { startHour: 11, endHour: 20 };
  }
  if (role === "주방") {
    return { startHour: 10, endHour: 19 };
  }
  return dayOffset % 2 === 0
    ? { startHour: 11, endHour: 17 }
    : { startHour: 17, endHour: 23 };
}

function startOfToday() {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  return now;
}

async function ensureBranch() {
  const existing = await prisma.branch.findUnique({
    where: { name: BRANCH_NAME },
  });
  if (existing) {
    return existing;
  }
  return prisma.branch.create({
    data: {
      name: BRANCH_NAME,
      areaType: BRANCH_AREA_TYPE,
    },
  });
}

async function upsertStaffByName(branchId, staffSeed) {
  const payload = {
    branchId,
    name: staffSeed.name,
    role: staffSeed.role,
    color: staffSeed.color,
    phone: staffSeed.phone,
    hourlyWage: staffSeed.hourlyWage,
    fixedDays: JSON.stringify(staffSeed.fixedDays),
    isActive: true,
  };

  const existing = await prisma.staff.findFirst({
    where: {
      branchId,
      name: staffSeed.name,
    },
  });

  if (existing) {
    return prisma.staff.update({
      where: { id: existing.id },
      data: payload,
    });
  }

  return prisma.staff.create({ data: payload });
}

async function ensureSchedule(staffId, startTime, endTime) {
  const existing = await prisma.schedule.findFirst({
    where: { staffId, startTime, endTime },
  });
  if (existing) {
    return false;
  }

  await prisma.schedule.create({
    data: {
      staffId,
      startTime,
      endTime,
      note: null,
    },
  });
  return true;
}

async function seedStaffData(days = 14) {
  console.log("🌱 직원 및 스케줄 샘플 데이터 생성 시작...\n");

  const branch = await ensureBranch();
  console.log(`✓ 지점 확인: ${branch.name} (${branch.id})`);

  console.log("\n👥 직원 데이터 업서트 중...");
  const staffRows = [];
  for (const seed of STAFF_SEED) {
    const staff = await upsertStaffByName(branch.id, seed);
    staffRows.push(staff);
    console.log(`  ✓ ${staff.name} (${staff.role})`);
  }

  console.log("\n📅 스케줄 생성 중...");
  const baseDate = startOfToday();
  let createdSchedules = 0;
  let skippedSchedules = 0;

  for (let dayOffset = 0; dayOffset < days; dayOffset++) {
    const scheduleDate = new Date(baseDate);
    scheduleDate.setDate(baseDate.getDate() + dayOffset);

    const dayName = DAY_NAMES[scheduleDate.getDay()];
    for (const staff of staffRows) {
      let fixedDays = [];
      try {
        fixedDays = JSON.parse(staff.fixedDays || "[]");
      } catch {
        fixedDays = [];
      }
      if (!fixedDays.includes(dayName)) {
        continue;
      }

      const { startHour, endHour } = getShift(staff.role, dayOffset);
      const startTime = new Date(scheduleDate);
      startTime.setHours(startHour, 0, 0, 0);

      const endTime = new Date(scheduleDate);
      endTime.setHours(endHour, 0, 0, 0);

      const created = await ensureSchedule(staff.id, startTime, endTime);
      if (created) {
        createdSchedules += 1;
      } else {
        skippedSchedules += 1;
      }
    }
  }

  console.log("\n✅ 샘플 데이터 처리 완료");
  console.log(`   - 직원(업서트): ${staffRows.length}명`);
  console.log(`   - 스케줄 신규: ${createdSchedules}개`);
  console.log(`   - 스케줄 중복 스킵: ${skippedSchedules}개`);
}

async function main() {
  try {
    await seedStaffData(14);
  } finally {
    await prisma.$disconnect();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = { main, seedStaffData };
