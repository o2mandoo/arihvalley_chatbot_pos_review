-- CreateTable
CREATE TABLE "sales_records" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "order_date" TEXT NOT NULL,
    "order_time" TEXT NOT NULL,
    "branch_name" TEXT NOT NULL,
    "area_type" TEXT NOT NULL,
    "menu_name" TEXT NOT NULL,
    "category" TEXT NOT NULL,
    "quantity" INTEGER NOT NULL,
    "price" DOUBLE PRECISION NOT NULL,
    "amount" DOUBLE PRECISION NOT NULL,
    "discount_amount" DOUBLE PRECISION NOT NULL,
    "payment_method" TEXT NOT NULL,
    "order_channel" TEXT NOT NULL,
    "order_status" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "sales_records_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "sales_records_order_date_idx" ON "sales_records"("order_date");

-- CreateIndex
CREATE INDEX "sales_records_branch_name_idx" ON "sales_records"("branch_name");

-- CreateIndex
CREATE INDEX "sales_records_category_idx" ON "sales_records"("category");

-- CreateIndex
CREATE INDEX "sales_records_order_status_idx" ON "sales_records"("order_status");
