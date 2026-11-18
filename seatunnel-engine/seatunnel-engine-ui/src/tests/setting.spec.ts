/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { setActivePinia, createPinia } from 'pinia'
import { useSettingStore } from '../store/setting/index'
import { beforeEach, describe, expect, it } from 'vitest'

describe('Setting Store', () => {
    beforeEach(() => {
        setActivePinia(createPinia())
    })

    it('getSequenceColumn', () => {
        const setting = useSettingStore()
        expect(setting.getSequenceColumn).equal(false)
    })
    it('themeMode follows the system by default and remembers a choice', () => {
        localStorage.removeItem('seatunnel.ui.theme')
        const setting = useSettingStore()
        expect(setting.getThemeMode).equal('system')
        setting.setThemeMode('dark')
        expect(setting.getThemeMode).equal('dark')
        expect(localStorage.getItem('seatunnel.ui.theme')).equal('dark')
        localStorage.removeItem('seatunnel.ui.theme')
    })
    it('requestTime', () => {
        const setting = useSettingStore()
        expect(setting.requestTime).equal(6000)
    })
})