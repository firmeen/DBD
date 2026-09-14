/* ============================================================================
   DBD DataWarehouse Ingestion — Data Inventory / รายงานสรุปข้อมูลที่ดึงมาได้
   ============================================================================

   ไฟล์นี้คืออะไร
     ชุด query สำหรับตอบคำถามว่า "ระบบดึงข้อมูลอะไรจาก DBD มาได้บ้าง
     เก็บไว้ที่ไหน ครบแค่ไหน และข้อมูลสดแค่ไหน"
     ไม่มีคำสั่งที่แก้ไขข้อมูล — เป็น SELECT ล้วน เปิดดูได้ปลอดภัย

   แหล่งข้อมูล
     datawarehouse.dbd.go.th (กรมพัฒนาธุรกิจการค้า)
     ดึงผ่าน HTTP request ล้วน ไม่ใช้ Selenium
     ข้อมูลเป็นข้อมูลสาธารณะที่ DBD เปิดเผยเพื่อบริการประชาชน

   วิธีใช้ใน DBeaver
     1. เลือก connection ที่ชี้ไปยัง database `dbd`
     2. รันบรรทัด SET @jp_no ด้านล่างก่อน 1 ครั้ง (Ctrl+Enter)
        ตัวแปรจะอยู่ตลอด session — ถ้าเปลี่ยน connection ต้องรันใหม่
     3. จากนั้นรันทีละ query ด้วย Ctrl+Enter
        (อย่ากด Alt+X รันทั้งไฟล์รวดเดียว เพราะจะได้ผลลัพธ์ 20 กว่าแท็บพร้อมกัน)

   สารบัญ
     ส่วนที่ 0  ตั้งค่าบริษัทที่ต้องการดู
     ส่วนที่ 1  ภาพรวมผู้บริหาร — เก็บอะไรไว้บ้าง ปริมาณเท่าไร
     ส่วนที่ 2  ความครบถ้วนรายบริษัท (completeness matrix)
     ส่วนที่ 3  ความสดของข้อมูล / audit trail
     ส่วนที่ 4  แท็บ 1 — ข้อมูลนิติบุคคล
     ส่วนที่ 5  แท็บ 1 — รายชื่อกรรมการ + กรรมการลงชื่อผูกพัน
     ส่วนที่ 6  แท็บ 2 — งบแสดงฐานะการเงิน
     ส่วนที่ 7  แท็บ 2 — งบกำไรขาดทุน
     ส่วนที่ 8  แท็บ 2 — อัตราส่วนทางการเงิน
     ส่วนที่ 9  แท็บ 3 — การลงทุนจำแนกตามสัญชาติ
     ส่วนที่ 10 แท็บ 4 — ประวัติการเปลี่ยนแปลง
     ส่วนที่ 11 ข้อมูลส่วนเกินที่เก็บไว้แต่เว็บไม่ได้แสดง
     ส่วนที่ 12 ตรวจสอบคุณภาพข้อมูล
     ส่วนที่ 13 ข้อจำกัด / สิ่งที่ยังไม่ได้ดึง  (อ่านก่อนรายงานหัวหน้า)
     ส่วนที่ 14 สถานะการดึงข้อมูลแบบ bulk (resume ได้)
   ========================================================================== */

USE dbd;


/* ============================================================================
   ส่วนที่ 0 — ตั้งค่าบริษัทที่ต้องการดู   << รันบรรทัดนี้ก่อนเสมอ >>
   ----------------------------------------------------------------------------
   jp_no = เลขทะเบียนนิติบุคคล 13 หลัก (ไม่ใช่ 14 หลักที่อยู่บน URL)
   URL /company/profile/70107556000051  ->  jp_type_code = 7, jp_no = 0107556000051
   ========================================================================== */

SET @jp_no = '0107556000051';


/* ============================================================================
   ส่วนที่ 1 — ภาพรวมผู้บริหาร
   ----------------------------------------------------------------------------
   คำถามที่ตอบ: "ระบบนี้เก็บอะไรไว้บ้าง และมีข้อมูลมากแค่ไหน"
   เหมาะสำหรับสไลด์แผ่นเดียวรายงานหัวหน้า
   ========================================================================== */

SELECT 'นิติบุคคล (บริษัท)'              AS หัวข้อ, COUNT(*) AS จำนวนแถว, 'companies'                  AS ตาราง FROM companies
UNION ALL SELECT 'รายชื่อกรรมการ',        COUNT(*), 'company_committees'        FROM company_committees
UNION ALL SELECT 'กรรมการลงชื่อผูกพัน',   COUNT(*), 'company_committee_signs'   FROM company_committee_signs
UNION ALL SELECT 'สัดส่วนผู้ถือหุ้นตามสัญชาติ', COUNT(*), 'company_nations'      FROM company_nations
UNION ALL SELECT 'ประวัติเปลี่ยนชื่อ',    COUNT(*), 'company_name_history'      FROM company_name_history
UNION ALL SELECT 'ประวัติเปลี่ยนทุน',     COUNT(*), 'company_capital_history'   FROM company_capital_history
UNION ALL SELECT 'แปรสภาพ/ควบรวม',        COUNT(*), 'company_transformations'   FROM company_transformations
UNION ALL SELECT 'สัญชาติย้อนหลัง 5 ปี',   COUNT(*), 'company_nation_history'    FROM company_nation_history
UNION ALL SELECT 'ประวัติการนำส่งงบ',     COUNT(*), 'financial_submissions'     FROM financial_submissions
UNION ALL SELECT 'งบการเงิน (รายปี)',     COUNT(*), 'financial_statements'      FROM financial_statements
UNION ALL SELECT 'รายการในงบการเงิน',     COUNT(*), 'financial_statement_items' FROM financial_statement_items
UNION ALL SELECT 'ค่าเฉลี่ยประเภทธุรกิจ',  COUNT(*), 'industry_benchmarks'       FROM industry_benchmarks
UNION ALL SELECT 'รายการค่าเฉลี่ยธุรกิจ',  COUNT(*), 'industry_benchmark_items'  FROM industry_benchmark_items
UNION ALL SELECT 'ฟิลด์เสริมนิติบุคคล',   COUNT(*), 'company_facts'             FROM company_facts
UNION ALL SELECT 'JSON ดิบจาก API (ทุกครั้งที่ดึง)', COUNT(*), 'raw_responses'  FROM raw_responses;


/* ----------------------------------------------------------------------------
   1.2 สรุปแบบ 1 บรรทัด — ใช้ตอบเร็ว ๆ ว่า "ตอนนี้มีข้อมูลกี่บริษัท กี่ปีงบ"
   -------------------------------------------------------------------------- */

SELECT
    (SELECT COUNT(*) FROM companies)                          AS จำนวนบริษัท,
    (SELECT COUNT(DISTINCT jp_no) FROM financial_statements)  AS บริษัทที่มีงบการเงิน,
    (SELECT COUNT(*) FROM financial_statements)               AS จำนวนปีงบทั้งหมด,
    (SELECT MIN(fiscal_year) FROM financial_statements)       AS ปีงบเก่าสุด,
    (SELECT MAX(fiscal_year) FROM financial_statements)       AS ปีงบล่าสุด,
    (SELECT COUNT(*) FROM raw_responses)                      AS จำนวนครั้งที่เรียก_API,
    (SELECT MAX(requested_at) FROM raw_responses)             AS ดึงข้อมูลล่าสุดเมื่อ;


/* ============================================================================
   ส่วนที่ 2 — ความครบถ้วนรายบริษัท (Completeness Matrix)
   ----------------------------------------------------------------------------
   คำถามที่ตอบ: "แต่ละบริษัทดึงมาครบทุกส่วนหรือยัง มีตัวไหนขาด"
   ถ้าคอลัมน์ไหนเป็น 0 = ส่วนนั้นยังไม่ได้ดึง หรือ DBD ไม่มีข้อมูลให้
   ========================================================================== */

SELECT
    c.jp_no                                                        AS เลขทะเบียน,
    c.jp_name                                                      AS ชื่อนิติบุคคล,
    c.jp_stat_desc                                                 AS สถานะ,
    (SELECT COUNT(*) FROM company_committees      x WHERE x.jp_no = c.jp_no) AS กรรมการ,
    (SELECT COUNT(*) FROM company_committee_signs x WHERE x.jp_no = c.jp_no) AS เงื่อนไขลงนาม,
    (SELECT COUNT(*) FROM company_nations         x WHERE x.jp_no = c.jp_no) AS สัญชาติ,
    (SELECT COUNT(*) FROM company_name_history    x WHERE x.jp_no = c.jp_no) AS ประวัติชื่อ,
    (SELECT COUNT(*) FROM company_capital_history x WHERE x.jp_no = c.jp_no) AS ประวัติทุน,
    (SELECT COUNT(*) FROM company_transformations x WHERE x.jp_no = c.jp_no) AS แปรสภาพ,
    (SELECT COUNT(*) FROM company_nation_history  x WHERE x.jp_no = c.jp_no) AS สัญชาติย้อนหลัง,
    (SELECT COUNT(*) FROM financial_submissions   x WHERE x.jp_no = c.jp_no) AS การนำส่งงบ,
    (SELECT COUNT(*) FROM financial_statements    x WHERE x.jp_no = c.jp_no) AS ปีงบที่มี,
    (SELECT COUNT(*) FROM financial_statement_items x WHERE x.jp_no = c.jp_no) AS รายการงบ,
    c.last_seen_at                                                 AS อัปเดตล่าสุด
FROM companies c
ORDER BY c.last_seen_at DESC;


/* ============================================================================
   ส่วนที่ 3 — ความสดของข้อมูล / Audit Trail
   ----------------------------------------------------------------------------
   จุดสำคัญที่หัวหน้าควรรู้:
   ทุกครั้งที่ดึง เราเก็บ JSON ดิบทั้งก้อนไว้ใน raw_responses แบบ append-only
   ไม่เคยลบ ไม่เคยทับ  ->  ถ้าวันหน้าต้องการฟิลด์เพิ่ม ดึงจากตารางนี้ได้เลย
   ไม่ต้องไปกวน server ของ DBD ซ้ำ และตรวจย้อนหลังได้ว่าวันนั้นได้ค่าอะไรมา
   ========================================================================== */

SELECT
    endpoint                   AS ชุดข้อมูล,
    COUNT(*)                   AS จำนวนครั้งที่ดึง,
    MIN(requested_at)          AS ดึงครั้งแรก,
    MAX(requested_at)          AS ดึงครั้งล่าสุด,
    COUNT(DISTINCT response_body_sha256) AS จำนวนเวอร์ชันที่ต่างกัน,
    ROUND(AVG(CHAR_LENGTH(raw_json)) / 1024, 1) AS ขนาดเฉลี่ย_KB
FROM raw_responses
GROUP BY endpoint
ORDER BY MAX(requested_at) DESC;

/* หมายเหตุการอ่านผล:
   "จำนวนเวอร์ชันที่ต่างกัน" = นับจาก SHA-256 ของ payload
   ถ้าดึง 3 ครั้งแต่ได้ 1 เวอร์ชัน แปลว่าข้อมูลฝั่ง DBD ยังไม่เปลี่ยนเลย */


/* ============================================================================
   ส่วนที่ 4 — แท็บ 1 "ข้อมูลนิติบุคคล"
   ----------------------------------------------------------------------------
   ตรงกับการ์ดซ้ายบนของหน้าเว็บ ทุกบรรทัด
   ========================================================================== */

SELECT
    c.jp_no                  AS `เลขทะเบียนนิติบุคคล`,
    c.jp_name                AS `ชื่อนิติบุคคล`,
    c.jp_name_e              AS `ชื่อภาษาอังกฤษ`,
    c.jp_type_desc           AS `ประเภทนิติบุคคล`,
    c.jp_stat_desc           AS `สถานะนิติบุคคล`,
    c.reg_date               AS `วันที่จดทะเบียนจัดตั้ง`,
    FORMAT(c.cap_amt, 2)     AS `ทุนจดทะเบียน`,
    FORMAT(c.paid_amt, 2)    AS `ทุนชำระแล้ว`,
    c.business_size_code     AS `ขนาดธุรกิจ`,
    c.submit_fin_year        AS `ปีที่ส่งงบการเงิน`,
    c.address                AS `ที่ตั้งสำนักงานแห่งใหญ่`,
    c.pv_desc                AS `จังหวัด`,
    c.zip_code               AS `รหัสไปรษณีย์`,
    COALESCE(c.website_1, '-') AS `Website`,
    c.setup_obj_code         AS `ประเภทธุรกิจตอนจดทะเบียน`,
    c.setup_obj_name_keyin   AS `วัตถุประสงค์ตอนจดทะเบียน`,
    c.update_date            AS `ข้อมูล_ณ_วันที่_ของ_DBD`,
    c.last_seen_at           AS `เราดึงล่าสุดเมื่อ`
FROM companies c
WHERE c.jp_no = @jp_no;

/* หมายเหตุ: รหัสทุกตัวมีคำอ่านกำกับแล้ว เช่น business_type_code 'D' ->
   business_type_desc 'บริการ' และ setup_obj_code '65110' -> 'การประกันชีวิต'
   คำอ่านพวกนี้ติดมากับ payload ของ profile อยู่แล้ว ไม่ต้องเรียก API เพิ่ม */


/* ----------------------------------------------------------------------------
   4.2 ข้อมูลการเปลี่ยนแปลงสถานะ — แปรสภาพ / ควบรวม
        (การ์ดล่างสุดของแท็บ 1 เช่น "นิติบุคคลนี้แปรสภาพมาจาก ...")
   -------------------------------------------------------------------------- */

SELECT
    CASE t.direction
        WHEN 'transform_from' THEN 'แปรสภาพมาจาก'
        WHEN 'transform_to'   THEN 'แปรสภาพไปเป็น'
        WHEN 'merger_from'    THEN 'ควบรวมมาจาก'
        WHEN 'merger_to'      THEN 'ควบรวมไปเป็น'
    END                      AS `ประเภทการเปลี่ยนแปลง`,
    t.change_date            AS `วันที่`,
    t.related_jp_name        AS `นิติบุคคลที่เกี่ยวข้อง`,
    t.related_jp_no          AS `เลขทะเบียนที่เกี่ยวข้อง`
FROM company_transformations t
WHERE t.jp_no = @jp_no
ORDER BY t.direction, t.seq_no;


/* ============================================================================
   ส่วนที่ 5 — แท็บ 1 "รายชื่อกรรมการ" และ "กรรมการลงชื่อผูกพัน"
   ========================================================================== */

SELECT
    cm.cmt_seq                                    AS `ลำดับ`,
    CONCAT_WS(' ', cm.title_name, cm.first_name, cm.last_name) AS `ชื่อกรรมการ`,
    cm.nt_code                                    AS `สัญชาติ`,
    cm.cmt_type_code                              AS `ประเภทกรรมการ`,
    cm.invest_amt                                 AS `เงินลงทุน`,
    cm.update_date                                AS `ปรับปรุงเมื่อ`
FROM company_committees cm
WHERE cm.jp_no = @jp_no
ORDER BY cm.cmt_seq;

/* หมายเหตุ: ตารางนี้เก็บ "เลขประจำตัวประชาชนกรรมการ" (cmt_no) ไว้ด้วย
   ซึ่งเป็นข้อมูลส่วนบุคคล จึงไม่ใส่ไว้ใน query ตัวอย่างข้างบน
   ถ้าต้องใช้ ควรพิจารณาเรื่อง PDPA และจำกัดสิทธิ์การเข้าถึงก่อน */

SELECT
    s.sign_seq          AS `ลำดับ`,
    s.sign_description  AS `เงื่อนไขกรรมการลงชื่อผูกพัน`
FROM company_committee_signs s
WHERE s.jp_no = @jp_no
ORDER BY s.sign_seq;


/* ============================================================================
   ส่วนที่ 6 — แท็บ 2 "งบแสดงฐานะการเงิน"
   ----------------------------------------------------------------------------
   นี่คือ query ที่ทำตารางบนหน้าเว็บออกมาได้เหมือนกันทุกช่อง
   แสดงจำนวนเงินแยกตามปีงบการเงิน (Buddhist year)
   ========================================================================== */

SELECT
    CASE i.item_code
        WHEN 'traderreceivables'         THEN '1. ลูกหนี้การค้าสุทธิ'
        WHEN 'inventories'               THEN '2. สินค้าคงเหลือ'
        WHEN 'currentassets'             THEN '3. สินทรัพย์หมุนเวียน'
        WHEN 'propertyplantandequipment' THEN '4. ที่ดิน อาคารและอุปกรณ์'
        WHEN 'noncurrentassets'          THEN '5. สินทรัพย์ไม่หมุนเวียน'
        WHEN 'assets'                    THEN '6. สินทรัพย์รวม'
        WHEN 'currentliabilities'        THEN '7. หนี้สินหมุนเวียน'
        WHEN 'noncurrentliabilities'     THEN '8. หนี้สินไม่หมุนเวียน'
        WHEN 'liabilities'               THEN '9. หนี้สินรวม'
        WHEN 'equity'                    THEN '10. ส่วนของผู้ถือหุ้น'
        WHEN 'liabilitiesandequity'      THEN '11. หนี้สินรวมและส่วนของผู้ถือหุ้น'
    END                                                              AS `รายการ (หน่วย : บาท)`,
    MAX(CASE WHEN i.fiscal_year = '2564' THEN i.amount END)          AS `2564`,
    MAX(CASE WHEN i.fiscal_year = '2565' THEN i.amount END)          AS `2565`,
    MAX(CASE WHEN i.fiscal_year = '2566' THEN i.amount END)          AS `2566`,
    MAX(CASE WHEN i.fiscal_year = '2567' THEN i.amount END)          AS `2567`,
    MAX(CASE WHEN i.fiscal_year = '2568' THEN i.amount END)          AS `2568`,
    MAX(CASE WHEN i.fiscal_year = '2568' THEN i.pct_change END)      AS `%เปลี่ยนแปลง 2568`
FROM financial_statement_items i
WHERE i.jp_no = @jp_no
  AND i.item_code IN ('traderreceivables','inventories','currentassets',
                      'propertyplantandequipment','noncurrentassets','assets',
                      'currentliabilities','noncurrentliabilities','liabilities',
                      'equity','liabilitiesandequity')
GROUP BY i.item_code
ORDER BY MIN(CASE i.item_code
        WHEN 'traderreceivables' THEN 1 WHEN 'inventories' THEN 2
        WHEN 'currentassets' THEN 3 WHEN 'propertyplantandequipment' THEN 4
        WHEN 'noncurrentassets' THEN 5 WHEN 'assets' THEN 6
        WHEN 'currentliabilities' THEN 7 WHEN 'noncurrentliabilities' THEN 8
        WHEN 'liabilities' THEN 9 WHEN 'equity' THEN 10 ELSE 11 END);

/* ถ้าบริษัทอื่นมีปีงบไม่ตรงกับ 2564-2568 ให้แก้ชื่อปีในคอลัมน์ข้างบน
   หรือใช้แบบ long-format ด้านล่างซึ่งไม่ยึดติดปี */


/* ----------------------------------------------------------------------------
   6.2 แบบ long-format — ใช้ได้ทุกบริษัท ไม่ต้องแก้ปี
       เหมาะกับการ export ไป Excel / Power BI
   -------------------------------------------------------------------------- */

SELECT
    i.fiscal_year   AS `ปีงบการเงิน`,
    i.item_code     AS `รหัสรายการ`,
    i.amount        AS `จำนวนเงิน`,
    i.amount_ly     AS `จำนวนเงินปีก่อน`,
    i.pct_change    AS `%เปลี่ยนแปลง`
FROM financial_statement_items i
WHERE i.jp_no = @jp_no
ORDER BY i.fiscal_year DESC, i.item_code;


/* ============================================================================
   ส่วนที่ 7 — แท็บ 2 "งบกำไรขาดทุน"
   ----------------------------------------------------------------------------
   ข้อมูลชุดนี้ได้มาฟรีพร้อมกับงบแสดงฐานะการเงิน เพราะ API ส่งมาก้อนเดียว
   (บนเว็บต้องกดปุ่มสลับ แต่เบื้องหลังคือ response เดียวกัน)
   ========================================================================== */

SELECT
    CASE i.item_code
        WHEN 'revfromsaleorrevfromservice' THEN '1. รายได้จากการขายหรือบริการ'
        WHEN 'revenue'                     THEN '2. รายได้รวม'
        WHEN 'costsofsaleofgoodorservice'  THEN '3. ต้นทุนขายหรือบริการ'
        WHEN 'grossprofit'                 THEN '4. กำไรขั้นต้น'
        WHEN 'sellingandserviceexpense'    THEN '5. ค่าใช้จ่ายในการขายและบริการ'
        WHEN 'administrativeexpense'       THEN '6. ค่าใช้จ่ายในการบริหาร'
        WHEN 'expenses'                    THEN '7. รวมค่าใช้จ่าย'
        WHEN 'interestexpense'             THEN '8. ดอกเบี้ยจ่าย'
        WHEN 'plbeforeincometaxexpense'    THEN '9. กำไร(ขาดทุน) ก่อนภาษี'
        WHEN 'taxexpenseincome'            THEN '10. ภาษีเงินได้'
        WHEN 'profitloss'                  THEN '11. กำไร(ขาดทุน) สุทธิ'
    END                                                        AS `รายการ (หน่วย : บาท)`,
    MAX(CASE WHEN i.fiscal_year = '2566' THEN i.amount END)    AS `2566`,
    MAX(CASE WHEN i.fiscal_year = '2567' THEN i.amount END)    AS `2567`,
    MAX(CASE WHEN i.fiscal_year = '2568' THEN i.amount END)    AS `2568`
FROM financial_statement_items i
WHERE i.jp_no = @jp_no
  AND i.item_code IN ('revfromsaleorrevfromservice','revenue','costsofsaleofgoodorservice',
                      'grossprofit','sellingandserviceexpense','administrativeexpense',
                      'expenses','interestexpense','plbeforeincometaxexpense',
                      'taxexpenseincome','profitloss')
GROUP BY i.item_code
ORDER BY MIN(CASE i.item_code
        WHEN 'revfromsaleorrevfromservice' THEN 1 WHEN 'revenue' THEN 2
        WHEN 'costsofsaleofgoodorservice' THEN 3 WHEN 'grossprofit' THEN 4
        WHEN 'sellingandserviceexpense' THEN 5 WHEN 'administrativeexpense' THEN 6
        WHEN 'expenses' THEN 7 WHEN 'interestexpense' THEN 8
        WHEN 'plbeforeincometaxexpense' THEN 9 WHEN 'taxexpenseincome' THEN 10 ELSE 11 END);

/* หมายเหตุ: บางรายการเป็น NULL ได้ตามปกติ เพราะธุรกิจบางประเภท
   (เช่น ประกันชีวิต ธนาคาร) ใช้รูปแบบงบเฉพาะ ไม่มี "กำไรขั้นต้น"
   NULL = DBD ไม่มีค่านี้ ไม่ใช่ระบบเราดึงพลาด */


/* ============================================================================
   ส่วนที่ 8 — แท็บ 2 "อัตราส่วนทางการเงิน"
   ----------------------------------------------------------------------------
   16 อัตราส่วน x ทุกปีงบ  ใช้ดูแนวโน้มสุขภาพทางการเงินได้ทันที
   ========================================================================== */

SELECT
    fs.fiscal_year                  AS `ปีงบ`,
    fs.business_size_desc           AS `ขนาดธุรกิจ`,
    fs.current_ratio                AS `อัตราส่วนสภาพคล่อง`,
    fs.debt_to_equity               AS `หนี้สินต่อทุน (D/E)`,
    fs.debt_to_asset                AS `หนี้สินต่อสินทรัพย์`,
    fs.gross_profit_margin          AS `อัตรากำไรขั้นต้น %`,
    fs.operating_profit_margin      AS `อัตรากำไรจากดำเนินงาน %`,
    fs.net_profit_margin            AS `อัตรากำไรสุทธิ %`,
    fs.return_on_asset              AS `ROA %`,
    fs.return_on_equity             AS `ROE %`,
    fs.earning_per_share            AS `กำไรต่อหุ้น (EPS)`,
    fs.total_asset_turnover         AS `การหมุนเวียนสินทรัพย์`,
    fs.account_receivable_turnover  AS `การหมุนเวียนลูกหนี้`,
    fs.inventory_turnover           AS `การหมุนเวียนสินค้าคงเหลือ`,
    fs.cost_to_income               AS `ต้นทุนต่อรายได้ %`
FROM financial_statements fs
WHERE fs.jp_no = @jp_no
ORDER BY fs.fiscal_year DESC;


/* ----------------------------------------------------------------------------
   8.2 ประวัติการนำส่งงบการเงิน — ส่งครบทุกปีไหม ส่งช้าไหม
   -------------------------------------------------------------------------- */

SELECT
    sub.submit_year   AS `ปีงบ`,
    sub.submit_date   AS `วันที่นำส่ง`,
    sub.ac_date       AS `วันปิดบัญชี_ววดด`,
    sub.status_code   AS `สถานะ`,
    sub.ac_code       AS `รหัสผู้ทำบัญชี`,
    sub.au_code       AS `รหัสผู้สอบบัญชี`
FROM financial_submissions sub
WHERE sub.jp_no = @jp_no
ORDER BY sub.submit_year DESC;


/* ============================================================================
   ส่วนที่ 9 — แท็บ 3 "การลงทุนจำแนกตามสัญชาติ"
   ========================================================================== */

SELECT
    n.nt_code           AS `รหัสสัญชาติ`,
    n.country_fullname  AS `สัญชาติ`,
    n.share_vol         AS `จำนวนหุ้น (หุ้น)`,
    n.share_amt         AS `มูลค่าการลงทุน (บาท)`,
    n.share_pct_vol     AS `สัดส่วนมูลค่าการลงทุน (%)`,
    n.share_qty         AS `จำนวนผู้ถือหุ้น (ราย)`,
    n.update_date       AS `ข้อมูล ณ วันที่`
FROM company_nations n
WHERE n.jp_no = @jp_no
ORDER BY n.share_amt DESC;

/* หมายเหตุ: ตารางนี้เป็น "ภาพ ณ ปัจจุบัน"
   ส่วนกราฟแท่ง 5 ปีย้อนหลังอยู่ที่ company_nation_history (ดู 9.2)
   แถว "รวมทุกสัญชาติ" ไม่ได้เก็บ เพราะคำนวณเองได้จาก SUM() */


/* ----------------------------------------------------------------------------
   9.2 กราฟสัดส่วนสัญชาติย้อนหลัง 5 ปี (ไทย vs ต่างชาติ)
   -------------------------------------------------------------------------- */

SELECT
    h.fiscal_year                                              AS `ปี`,
    MAX(CASE WHEN h.nation_group = 'TH'    THEN h.share_pct END) AS `ไทย %`,
    MAX(CASE WHEN h.nation_group = 'OTHER' THEN h.share_pct END) AS `ต่างชาติ %`,
    MAX(CASE WHEN h.nation_group = 'TH'    THEN h.share_amt END) AS `มูลค่าไทย (บาท)`,
    MAX(CASE WHEN h.nation_group = 'OTHER' THEN h.share_amt END) AS `มูลค่าต่างชาติ (บาท)`
FROM company_nation_history h
WHERE h.jp_no = @jp_no
GROUP BY h.fiscal_year
ORDER BY h.fiscal_year;

/* ค่า NULL ในคอลัมน์ต่างชาติ = ไม่มีผู้ถือหุ้นต่างชาติในปีนั้น */


/* ----------------------------------------------------------------------------
   9.3 เทียบบริษัทกับค่าเฉลี่ยในประเภทธุรกิจเดียวกัน
        (ตรงกับ toggle "เปรียบเทียบในประเภทธุรกิจ" บนแท็บ 2)

        สำคัญ: ตาราง industry_benchmarks เก็บตาม "ประเภทธุรกิจ" ไม่ใช่ตามบริษัท
        ดังนั้นบริษัทหลายรายที่อยู่ TSIC เดียวกันจะใช้แถวเดียวกัน
   -------------------------------------------------------------------------- */

SELECT
    b.submit_obj_code       AS `รหัสประเภทธุรกิจ`,
    c.submit_obj_desc       AS `ประเภทธุรกิจ`,
    b.fiscal_year           AS `ปีงบ`,
    b.business_size_code    AS `ขนาด`,
    b.submit_fin_cnt        AS `จำนวนบริษัทในกลุ่ม`,
    b.profit_cnt            AS `บริษัทที่กำไร`,
    b.loss_cnt              AS `บริษัทที่ขาดทุน`,
    i.item_code             AS `รายการ`,
    own.amount              AS `บริษัทนี้`,
    i.avg_amount            AS `ค่าเฉลี่ยในกลุ่ม`,
    ROUND(own.amount - i.avg_amount, 2)                          AS `ต่างจากค่าเฉลี่ย`,
    i.industry_total        AS `รวมทั้งกลุ่ม`
FROM companies c
JOIN industry_benchmarks      b  ON b.submit_obj_code = c.submit_obj_code
JOIN industry_benchmark_items i  ON i.benchmark_id = b.id
LEFT JOIN financial_statement_items own
       ON own.jp_no = c.jp_no AND own.fiscal_year = b.fiscal_year AND own.item_code = i.item_code
WHERE c.jp_no = @jp_no
  AND i.item_code IN ('assets','liabilities','equity','revenue','profitloss')
ORDER BY i.item_code;


/* ============================================================================
   ส่วนที่ 10 — แท็บ 4 "ประวัติการเปลี่ยนแปลง"
   ========================================================================== */

SELECT
    nh.seq_desc     AS `ลำดับที่`,
    nh.change_date  AS `วันที่เปลี่ยนแปลง`,
    nh.jp_new_name  AS `ชื่อนิติบุคคล`
FROM company_name_history nh
WHERE nh.jp_no = @jp_no
ORDER BY nh.change_date;

/* ประวัติการเปลี่ยนแปลงทุนจดทะเบียน
   สำคัญ: คอลัมน์ "ทุนจดทะเบียน" ที่เว็บแสดง ตรงกับ previous_cap_amt
   ไม่ใช่ new_cap_amt — ตรวจสอบแล้วตรงกับหน้าเว็บทั้ง 4 แถว */

SELECT
    ch.seq_desc          AS `ลำดับที่`,
    ch.change_date       AS `วันที่เปลี่ยนแปลง`,
    ch.previous_cap_amt  AS `ทุนจดทะเบียน (บาท)`,
    ch.inc_dec_amt       AS `จำนวนที่เพิ่ม/ลด`,
    ch.new_cap_amt       AS `ทุนหลังเปลี่ยน (อ้างอิง)`
FROM company_capital_history ch
WHERE ch.jp_no = @jp_no
ORDER BY ch.change_date;


/* ============================================================================
   ส่วนที่ 11 — ข้อมูลส่วนเกินที่เก็บไว้ แต่หน้าเว็บไม่ได้แสดง
   ----------------------------------------------------------------------------
   จุดขายของระบบ: เราเก็บทุกฟิลด์ที่ API ส่งมา ไม่ใช่แค่ที่ตาเห็นบนหน้าจอ
   ฟิลด์พวกนี้ตกอยู่ใน company_facts ทั้งหมด (ประมาณ 46 ฟิลด์ต่อบริษัท)
   ========================================================================== */

SELECT
    f.fact_key    AS `ชื่อฟิลด์`,
    f.fact_value  AS `ค่า`
FROM company_facts f
WHERE f.jp_no = @jp_no
  AND f.raw_response_id = (SELECT c.latest_raw_response_id FROM companies c WHERE c.jp_no = @jp_no)
ORDER BY f.fact_key;

/* ตัวอย่างของดีที่อยู่ในนี้:
     jpAge                 อายุบริษัท (ปี)
     jpNameOld             ชื่อเดิมก่อนเปลี่ยน
     currentRatio, debtToEquity, returnOnAsset, returnOnEquity
                           อัตราส่วนการเงินปีล่าสุด (สรุปมาให้แล้ว)
     totalIncomePctGrowth  % การเติบโตของรายได้
     eximFlag              เป็นผู้ประกอบการนำเข้า/ส่งออกหรือไม่
     frnLicenseFlag        มีใบอนุญาตประกอบธุรกิจคนต่างด้าวหรือไม่
     cacFlag               เป็นสมาชิกแนวร่วมต่อต้านคอร์รัปชัน (CAC) หรือไม่
     dbdERegFlag           จดทะเบียนพาณิชย์อิเล็กทรอนิกส์กับ DBD หรือไม่ */


/* ============================================================================
   ส่วนที่ 12 — ตรวจสอบคุณภาพข้อมูล (Data Quality)
   ----------------------------------------------------------------------------
   รันชุดนี้ก่อนส่งข้อมูลให้ใครใช้ต่อ
   ========================================================================== */

/* 12.1 บริษัทที่ยังไม่มีงบการเงิน — อาจตั้งใหม่ หรือดึงไม่ครบ */
SELECT c.jp_no AS เลขทะเบียน, c.jp_name AS ชื่อ, c.jp_stat_desc AS สถานะ, c.reg_date AS วันจดทะเบียน
FROM companies c
LEFT JOIN financial_statements fs ON fs.jp_no = c.jp_no
WHERE fs.jp_no IS NULL;

/* 12.2 ตรวจสมการบัญชี: สินทรัพย์รวม = หนี้สินรวม + ส่วนของผู้ถือหุ้น
        ถ้า ผลต่าง != 0 แปลว่าข้อมูลต้นทางผิด หรือเราแม็ปผิด */
SELECT
    a.fiscal_year                          AS `ปีงบ`,
    a.amount                               AS `สินทรัพย์รวม`,
    l.amount                               AS `หนี้สินรวม`,
    e.amount                               AS `ส่วนของผู้ถือหุ้น`,
    (l.amount + e.amount)                  AS `หนี้สิน+ทุน`,
    (a.amount - (l.amount + e.amount))     AS `ผลต่าง (ควรเป็น 0)`
FROM      financial_statement_items a
LEFT JOIN financial_statement_items l ON l.jp_no = a.jp_no AND l.fiscal_year = a.fiscal_year AND l.item_code = 'liabilities'
LEFT JOIN financial_statement_items e ON e.jp_no = a.jp_no AND e.fiscal_year = a.fiscal_year AND e.item_code = 'equity'
WHERE a.jp_no = @jp_no AND a.item_code = 'assets'
ORDER BY a.fiscal_year;

/* 12.3 จำนวนรายการงบต่อปี — ควรเท่ากันทุกปี ถ้าปีไหนน้อยผิดปกติให้ตรวจสอบ */
SELECT fiscal_year AS `ปีงบ`, COUNT(*) AS `จำนวนรายการ`,
       SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS `รายการที่ไม่มีค่า`
FROM financial_statement_items
WHERE jp_no = @jp_no
GROUP BY fiscal_year
ORDER BY fiscal_year;


/* ============================================================================
   ส่วนที่ 13 — ข้อจำกัด / สิ่งที่ยังไม่ได้ดึง
   ============================================================================

   >>> อ่านส่วนนี้ก่อนรายงานหัวหน้า เพื่อไม่ให้เคลมเกินจริง <<<

   ดึงครบแล้ว (ครบทั้ง 4 แท็บหลัก):
     [x] ข้อมูลนิติบุคคล + รายชื่อกรรมการ + กรรมการลงชื่อผูกพัน
     [x] ข้อมูลการเปลี่ยนแปลงสถานะ (แปรสภาพ / ควบรวม)
     [x] งบการเงิน: งบแสดงฐานะการเงิน + งบกำไรขาดทุน + อัตราส่วน (ครบทุกปีที่ส่ง)
     [x] ประวัติการนำส่งงบการเงิน
     [x] ค่าเฉลี่ยเปรียบเทียบในประเภทธุรกิจเดียวกัน
     [x] การลงทุนจำแนกตามสัญชาติ (ภาพปัจจุบัน + ย้อนหลัง 5 ปี)
     [x] ประวัติการเปลี่ยนชื่อ + ประวัติการเปลี่ยนแปลงทุน
     [x] คำอ่านของรหัสทุกตัว (65110 -> การประกันชีวิต, D -> บริการ,
         รวมถึงจังหวัด/อำเภอ/ตำบล) ได้มาพร้อม payload ไม่ต้องเรียก API เพิ่ม

   ยังไม่ได้ดึง (รู้ endpoint แล้ว ต่อเพิ่มได้):
     [ ] ผู้ชำระบัญชี (ใช้เฉพาะบริษัทที่เลิกกิจการ — คืน 404 ถ้ายังดำเนินกิจการ)
     [ ] รายชื่อหุ้นส่วน (ใช้กับ หจก. ไม่ใช่ บมจ.)
     [ ] ใบอนุญาต ธปท./ก.ล.ต./เครื่องหมายการค้า (endpoint /ext/*)

   ยังไม่ได้แตะเลย (อีก 2 แท็บบนหน้าเว็บ):
     [ ] รางวัล/สัญลักษณ์
     [ ] ข้อมูลหน่วยงานภายนอก (ใบอนุญาต ธปท. / ก.ล.ต. / เครื่องหมายการค้า)

   ข้อจำกัดเชิงเทคนิคที่ควรรู้:
     - ปีงบการเงินเป็น พ.ศ. (2564-2568) เก็บเป็น VARCHAR ไม่ใช่ตัวเลข
     - ข้อมูลจาก DBD เป็น "ข้อมูลทั่วไปที่เปิดเผยเพื่อบริการประชาชน"
       ตามหมายเหตุของกรมฯ ไม่สามารถนำไปใช้อ้างอิงตามกฎหมายได้
     - ข้อมูลอัปเดตตามรอบของ DBD (ดูคอลัมน์ update_date)
       ไม่ใช่ real-time
     - ตาราง company_facts เก็บแบบ append ทุกครั้งที่ดึง
       ถ้าต้องการเฉพาะค่าล่าสุด ต้อง filter ด้วย latest_raw_response_id
       (ดูตัวอย่างในส่วนที่ 11)
   ========================================================================== */


/* ============================================================================
   ส่วนที่ 14 — สถานะการดึงข้อมูลแบบ bulk
   ----------------------------------------------------------------------------
   ระบบดึงหลายบริษัทพร้อมกันได้ และ resume ได้ถ้าโปรแกรมหยุดกลางคัน
   สถานะถูก commit ทีละบริษัท -> ถ้า Ctrl+C หรือเน็ตหลุด เสียแค่บริษัทเดียว
   ========================================================================== */

SELECT
    r.id                                   AS `run`,
    r.source_label                         AS `ไฟล์ต้นทาง`,
    r.parts                                AS `ส่วนที่ดึง`,
    r.total_tasks                          AS `ทั้งหมด`,
    SUM(t.status = 'done')                 AS `สำเร็จ`,
    SUM(t.status = 'failed')               AS `ล้มเหลว`,
    SUM(t.status = 'pending')              AS `รอดำเนินการ`,
    r.created_at                           AS `เริ่มเมื่อ`,
    r.finished_at                          AS `เสร็จเมื่อ`
FROM ingestion_runs r
LEFT JOIN ingestion_tasks t ON t.run_id = r.id
GROUP BY r.id
ORDER BY r.id DESC;

/* รายชื่อบริษัทที่ดึงไม่สำเร็จ พร้อมสาเหตุ */
SELECT
    t.run_id        AS `run`,
    t.company_code  AS `รหัสบริษัท`,
    t.attempts      AS `พยายามกี่ครั้ง`,
    t.last_error    AS `สาเหตุ`,
    t.finished_at   AS `เมื่อ`
FROM ingestion_tasks t
WHERE t.status = 'failed'
ORDER BY t.run_id DESC, t.id;

/* สั่งลองใหม่เฉพาะตัวที่ล้มเหลว:  dbd-data bulk-retry <run_id>  */
