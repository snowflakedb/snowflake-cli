/*
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

CREATE OR ALTER TABLE TABLE_TEST (
    A NUMBER(38,0) NOT NULL,
    B NUMBER(38,0) NOT NULL,
    C DATE,
    D NUMBER(38,0),
    E VARCHAR(16777216) COLLATE 'EN-CI',
    F NUMBER(38,0),
    G VARCHAR(16777216) COLLATE 'EN-CI',
    H VARCHAR(16777216) COLLATE 'EN-CI',
    I VARCHAR(16777216) COLLATE 'EN-CI',
    J NUMBER(38,0),
    K VARCHAR(16777216) COLLATE 'EN-CI',
    L NUMBER(38,0),
    M NUMBER(38,0),
    N NUMBER(38,0),
    O NUMBER(38,0),
    P TIMESTAMP_NTZ(9) NOT NULL,
    R VARCHAR(16777216) NOT NULL COLLATE 'EN-CI',
    );
