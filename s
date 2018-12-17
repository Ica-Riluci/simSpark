[33mcommit b3f8c8ac0e0a67c97ddd5390a45ce9cdf9c53500[m[33m ([m[1;36mHEAD -> [m[1;32mmaster[m[33m, [m[1;33mtag: valpha-0[m[33m, [m[1;31morigin/master[m[33m, [m[1;31morigin/HEAD[m[33m)[m
Author: Ica Riluci <weiyh15@mails.tsinghua.edu.cn>
Date:   Tue Dec 18 00:44:39 2018 +0800

    Initial commit

[1mdiff --git a/LICENSE b/LICENSE[m
[1mnew file mode 100644[m
[1mindex 0000000..261eeb9[m
[1m--- /dev/null[m
[1m+++ b/LICENSE[m
[36m@@ -0,0 +1,201 @@[m
[32m+[m[32m                                 Apache License[m
[32m+[m[32m                           Version 2.0, January 2004[m
[32m+[m[32m                        http://www.apache.org/licenses/[m
[32m+[m
[32m+[m[32m   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION[m
[32m+[m
[32m+[m[32m   1. Definitions.[m
[32m+[m
[32m+[m[32m      "License" shall mean the terms and conditions for use, reproduction,[m
[32m+[m[32m      and distribution as defined by Sections 1 through 9 of this document.[m
[32m+[m
[32m+[m[32m      "Licensor" shall mean the copyright owner or entity authorized by[m
[32m+[m[32m      the copyright owner that is granting the License.[m
[32m+[m
[32m+[m[32m      "Legal Entity" shall mean the union of the acting entity and all[m
[32m+[m[32m      other entities that control, are controlled by, or are under common[m
[32m+[m[32m      control with that entity. For the purposes of this definition,[m
[32m+[m[32m      "control" means (i) the power, direct or indirect, to cause the[m
[32m+[m[32m      direction or management of such entity, whether by contract or[m
[32m+[m[32m      otherwise, or (ii) ownership of fifty percent (50%) or more of the[m
[32m+[m[32m      outstanding shares, or (iii) beneficial ownership of such entity.[m
[32m+[m
[32m+[m[32m      "You" (or "Your") shall mean an individual or Legal Entity[m
[32m+[m[32m      exercising permissions granted by this License.[m
[32m+[m
[32m+[m[32m      "Source" form shall mean the preferred form for making modifications,[m
[32m+[m[32m      including but not limited to software source code, documentation[m
[32m+[m[32m      source, and configuration files.[m
[32m+[m
[32m+[m[32m      "Object" form shall mean any form resulting from mechanical[m
[32m+[m[32m      transformation or translation of a Source form, including but[m
[32m+[m[32m      not limited to compiled object code, generated documentation,[m
[32m+[m[32m      and conversions to other media types.[m
[32m+[m
[32m+[m[32m      "Work" shall mean the work of authorship, whether in Source or[m
[32m+[m[32m      Object form, made available under the License, as indicated by a[m
[32m+[m[32m      copyright notice that is included in or attached to the work[m
[32m+[m[32m      (an example is provided in the Appendix below).[m
[32m+[m
[32m+[m[32m      "Derivative Works" shall mean any work, whether in Source or Object[m
[32m+[m[32m      form, that is based on (or derived from) the Work and for which the[m
[32m+[m[32m      editorial revisions, annotations, elaborations, or other modifications[m
[32m+[m[32m      represent, as a whole, an original work of authorship. For the purposes[m
[32m+[m[32m      of this License, Derivative Works shall not include works that remain[m
[32m+[m[32m      separable from, or merely link (or bind by name) to the interfaces of,[m
[32m+[m[32m      the Work and Derivative Works thereof.[m
[32m+[m
[32m+[m[32m      "Contribution" shall mean any work of authorship, including[m
[32m+[m[32m      the original version of the Work and any modifications or additions[m
[32m+[m[32m      to that Work or Derivative Works thereof, that is intentionally[m
[32m+[m[32m      submitted to Licensor for inclusion in the Work by the copyright owner[m
[32m+[m[32m      or by an individual or Legal Entity authorized to submit on behalf of[m
[32m+[m[32m      the copyright owner. For the purposes of this definition, "submitted"[m
[32m+[m[32m      means any form of electronic, verbal, or written communication sent[m
[32m+[m[32m      to the Licensor or its representatives, including but not limited to[m
[32m+[m[32m      communication on electronic mailing lists, source code control systems,[m
[32m+[m[32m      and issue tracking systems that are managed by, or on behalf of, the[m
[32m+[m[32m      Licensor for the purpose of discussing and improving the Work, but[m
[32m+[m[32m      excluding communication that is conspicuously marked or otherwise[m
[32m+[m[32m      designated in writing by the copyright owner as "Not a Contribution."[m
[32m+[m
[32m+[m[32m      "Contributor" shall mean Licensor and any individual or Legal Entity[m
[32m+[m[32m      on behalf of whom a Contribution has been received by Licensor and[m
[32m+[m[32m      subsequently incorporated within the Work.[m
[32m+[m
[32m+[m[32m   2. Grant of Copyright License. Subject to the terms and conditions of[m
[32m+[m[32m      this License, each Contributor hereby grants to You a perpetual,[m
[32m+[m[32m      worldwide, non-exclusive, no-charge, royalty-free, irrevocable[m
[32m+[m[32m      copyright license to reproduce, prepare Derivative Works of,[m
[32m+[m[32m      publicly display, publicly perform, sublicense, and distribute the[m
[32m+[m[32m      Work and such Derivative Works in Source or Object form.[m
[32m+[m
[32m+[m[32m   3. Grant of Patent License. Subject to the terms and conditions of[m
[32m+[m[32m      this License, each Contributor hereby grants to You a perpetual,[m
[32m+[m[32m      worldwide, non-exclusive, no-charge, royalty-free, irrevocable[m
[32m+[m[32m      (except as stated in this section) patent license to make, have made,[m
[32m+[m[32m      use, offer to sell, sell, import, and otherwise transfer the Work,[m
[32m+[m[32m      where such license applies only to those patent claims licensable[m
[32m+[m[32m      by such Contributor that are necessarily infringed by their[m
[32m+[m[32m      Contribution(s) alone or by combination of their Contribution(s)[m
[32m+[m[32m      with the Work to which such Contribution(s) was submitted. If You[m
[32m+[m[32m      institute patent litigation against any entity (including a[m
[32m+[m[32m      cross-claim or counterclaim in a lawsuit) alleging that the Work[m
[32m+[m[32m      or a Contribution incorporated within the Work constitutes direct[m
[32m+[m[32m      or contributory patent infringement, then any patent licenses[m
[32m+[m[32m      granted to You under this License for that Work shall terminate[m
[32m+[m[32m      as of the date such litigation is filed.[m
[32m+[m
[32m+[m[32m   4. Redistribution. You may reproduce and distribute copies of the[m
[32m+[m[32m      Work or Derivative Works thereof in any medium, with or without[m
[32m+[m[32m      modifications, and in Source or Object form, provided that You[m
[32m+[m[32m      meet the following conditions:[m
[32m+[m
[32m+[m[32m      (a) You must give any other recipients of the Work or[m
[32m+[m[32m          Derivative Works a copy of this License; and[m
[32m+[m
[32m+[m[32m      (b) You must cause any modified files to carry prominent notices[m
[32m+[m[32m          stating that You changed the files; and[m
[32m+[m
[32m+[m[32m      (c) You must retain, in the Source form of any Derivative Works[m
[32m+[m[32m          that You distribute, all copyright, patent, trademark, and[m
[32m+[m[32m          attribution notices from the Source form of the Work,[m
[32m+[m[32m          excluding those notices that do not pertain to any part of[m
[32m+[m[32m          the Derivative Works; and[m
[32m+[m
[32m+[m[32m      (d) If the Work includes a "NOTICE" text file as part of its[m
[32m+[m[32m          distribution, then any Derivative Works that You distribute must[m
[32m+[m[32m          include a readable copy of the attribution notices contained[m
[32m+[m[32m          within such NOTICE file, excluding those notices that do not[m
[32m+[m[32m          pertain to any part of the Derivative Works, in at least one[m
[32m+[m[32m          of the following places: within a NOTICE text file distributed[m
[32m+[m[32m          as part of the Derivative Works; within the Source form or[m
[32m+[m[32m          documentation, if provided along with the Derivative Works; or,[m
[32m+[m[32m          within a display generated by the Derivative Works, if and[m
[32m+[m[32m          wherever such third-party notices normally appear. The contents[m
[32m+[m[32m          of the NOTICE file are for informational purposes only and[m
[32m+[m[32m          do not modify the License. You may add Your own attribution[m
[32m+[m[32m          notices within Derivative Works that You distribute, alongside[m
[32m+[m[32m          or as an addendum to the NOTICE text from the Work, provided[m
[32m+[m[32m          that such additional attribution notices cannot be construed[m
[32m+[m[32m          as modifying the License.[m
[32m+[m
[32m+[m[32m      You may add Your own copyright statement to Your modifications and[m
[32m+[m[32m      may provide additional or different license terms and conditions[m
[32m+[m[32m      for use, reproduction, or distribution of Your modifications, or[m
[32m+[m[32m      for any such Derivative Works as a whole, provided Your use,[m
[32m+[m[32m      reproduction, and distribution of the Work otherwise complies with[m
[32m+[m[32m      the conditions stated in this License.[m
[32m+[m
[32m+[m[32m   5. Submission of Contributions. Unless You explicitly state otherwise,[m
[32m+[m[32m      any Contribution intentionally submitted for inclusion in the Work[m
[32m+[m[32m      by You to the Licensor shall be under the terms and conditions of[m
[32m+[m[32m      this License, without any additional terms or conditions.[m
[32m+[m[32m      Notwithstanding the above, nothing herein shall supersede or modify[m
[32m+[m[32m      the terms of any separate license agreement you may have executed[m
[32m+[m[32m      with Licensor regarding such Contributions.[m
[32m+[m
[32m+[m[32m   6. Trademarks. This License does not grant permission to use the trade[m
[32m+[m[32m      names, trademarks, service marks, or product names of the Licensor,[m
[32m+[m[32m      except as required for reasonable and customary use in describing the[m
[32m+[m[32m      origin of the Work and reproducing the content of the NOTICE file.[m
[32m+[m
[32m+[m[32m   7. Disclaimer of Warranty. Unless required by applicable law or[m
[32m+[m[32m      agreed to in writing, Licensor provides the Work (and each[m
[32m+[m[32m      Contributor provides its Contributions) on an "AS IS" BASIS,[m
[32m+[m[32m      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or[m
[32m+[m[32m      implied, including, without limitation, any warranties or conditions[m
[32m+[m[32m      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A[m
[32m+[m[32m      PARTICULAR PURPOSE. You are solely responsible for determining the[m
[32m+[m[32m      appropriateness of using or redistributing the Work and assume any[m
[32m+[m[32m      risks associated with Your exercise of permissions under this License.[m
[32m+[m
[32m+[m[32m   8. Limitation of Liability. In no event and under no legal theory,[m
[32m+[m[32m      whether in tort (including negligence), contract, or otherwise,[m
[32m+[m[32m      unless required by applicable law (such as deliberate and grossly[m
[32m+[m[32m      negligent acts) or agreed to in writing, shall any Contributor be[m
[32m+[m[32m      liable to You for damages, including any direct, indirect, special,[m
[32m+[m[32m      incidental, or consequential damages of any character arising as a[m
[32m+[m[32m      result of this License or out of the use or inability to use the[m
[32m+[m[32m      Work (including but not limited to damages for loss of goodwill,[m
[32m+[m[32m      work stoppage, computer failure or malfunction, or any and all[m
[32m+[m[32m      other commercial damages or losses), even if such Contributor[m
[32m+[m[32m      has been advised of the possibility of such damages.[m
[32m+[m
[32m+[m[32m   9. Accepting Warranty or Additional Liability. While redistributing[m
[32m+[m[32m      the Work or Derivative Works thereof, You may choose to offer,[m
[32m+[m[32m      and charge a fee for, acceptance of support, warranty, indemnity,[m
[32m+[m[32m      or other liability obligations and/or rights consistent with this[m
[32m+[m[32m      License. However, in accepting such obligations, You may act only[m
[32m+[m[32m      on Your own behalf and on Your sole responsibility, not on behalf[m
[32m+[m[32m      of any other Contributor, and only if You agree to indemnify,[m
[32m+[m[32m      defend, and hold each Contributor harmless for any liability[m
[32m+[m[32m      incurred by, or claims asserted against, such Contributor by reason[m
[32m+[m[32m      of your accepting any such warranty or additional liability.[m
[32m+[m
[32m+[m[32m   END OF TERMS AND CONDITIONS[m
[32m+[m
[32m+[m[32m   APPENDIX: How to apply the Apache License to your work.[m
[32m+[m
[32m+[m[32m      To apply the Apache License to your work, attach the following[m
[32m+[m[32m      boilerplate notice, with the fields enclosed by brackets "[]"[m
[32m+[m[32m      replaced with your own identifying information. (Don't include[m
[32m+[m[32m      the brackets!)  The text should be enclosed in the appropriate[m
[32m+[m[32m      comment syntax for the file format. We also recommend that a[m
[32m+[m[32m      file or class name and description of purpose be included on the[m
[32m+[m[32m      same "printed page" as the copyright notice for easier[m
[32m+[m[32m      identification within third-party archives.[m
[32m+[m
[32m+[m[32m   Copyright [yyyy] [name of copyright owner][m
[32m+[m
[32m+[m[32m   Licensed under the Apache License, Version 2.0 (the "License");[m
[32m+[m[32m   you may not use this file except in compliance with the License.[m
[32m+[m[32m   You may obtain a copy of the License at[m
[32m+[m
[32m+[m[32m       http://www.apache.org/licenses/LICENSE-2.0[m
[32m+[m
[32m+[m[32m   Unless required by applicable law or agreed to in writing, software[m
[32m+[m[32m   distributed under the License is distributed on an "AS IS" BASIS,[m
[32m+[m[32m   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.[m
[32m+[m[32m   See the License for the specific language governing permissions and[m
[32m+[m[32m   limitations under the License.[m
[1mdiff --git a/README.md b/README.md[m
[1mnew file mode 100644[m
[1mindex 0000000..9bed9d4[m
[1m--- /dev/null[m
[1m+++ b/README.md[m
[36m@@ -0,0 +1,2 @@[m
[32m+[m[32m# simSpark[m
[32m+[m[32ma simple mini Spark project[m
