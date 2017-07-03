# -*- coding: utf-8 -*-

"""
    sphinx.util.texescape
    ~~~~~~~~~~~~~~~~~~~~~
    TeX escaping helper.
    :copyright: Copyright 2007-2017 by the Sphinx team, see AUTHORS.
    :license: BSD, see LICENSE for details.
"""

"""
Copyright (c) 2007-2017 by the Sphinx team (see AUTHORS file).
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

tex_replacements = [
    # map TeX special chars
    ('$', r'\$'),
    ('%', r'\%'),
    ('&', r'\&'),
    ('#', r'\#'),
    ('_', r'\_'),
    ('{', r'\{'),
    ('}', r'\}'),
    ('[', r'{[}'),
    (']', r'{]}'),
    ('`', r'{}`'),
    ('\\', r'\textbackslash{}'),
    ('~', r'\textasciitilde{}'),
    ('<', r'\textless{}'),
    ('>', r'\textgreater{}'),
    ('^', r'\textasciicircum{}'),
    # map special Unicode characters to TeX commands
    ('¶', r'\P{}'),
    ('§', r'\S{}'),
    ('€', r'\texteuro{}'),
    ('∞', r'\(\infty\)'),
    ('±', r'\(\pm\)'),
    ('→', r'\(\rightarrow\)'),
    ('‣', r'\(\rightarrow\)'),
    ('✓', r'\(\checkmark\)'),
    # used to separate -- in options
    ('﻿', r'{}'),
    # map some special Unicode characters to similar ASCII ones
    ('─', r'-'),
    ('⎽', r'\_'),
    ('╲', r'\textbackslash{}'),
    ('–', r'\textendash{}'),
    ('|', r'\textbar{}'),
    ('│', r'\textbar{}'),
    ('ℯ', r'e'),
    ('ⅈ', r'i'),
    ('⁰', r'\(\sp{\text{0}}\)'),
    ('¹', r'\(\sp{\text{1}}\)'),
    ('²', r'\(\sp{\text{2}}\)'),
    ('³', r'\(\sp{\text{3}}\)'),
    ('⁴', r'\(\sp{\text{4}}\)'),
    ('⁵', r'\(\sp{\text{5}}\)'),
    ('⁶', r'\(\sp{\text{6}}\)'),
    ('⁷', r'\(\sp{\text{7}}\)'),
    ('⁸', r'\(\sp{\text{8}}\)'),
    ('⁹', r'\(\sp{\text{9}}\)'),
    ('₀', r'\(\sb{\text{0}}\)'),
    ('₁', r'\(\sb{\text{1}}\)'),
    ('₂', r'\(\sb{\text{2}}\)'),
    ('₃', r'\(\sb{\text{3}}\)'),
    ('₄', r'\(\sb{\text{4}}\)'),
    ('₅', r'\(\sb{\text{5}}\)'),
    ('₆', r'\(\sb{\text{6}}\)'),
    ('₇', r'\(\sb{\text{7}}\)'),
    ('₈', r'\(\sb{\text{8}}\)'),
    ('₉', r'\(\sb{\text{9}}\)'),
    # map Greek alphabet
    ('α', r'\(\alpha\)'),
    ('β', r'\(\beta\)'),
    ('γ', r'\(\gamma\)'),
    ('δ', r'\(\delta\)'),
    ('ε', r'\(\epsilon\)'),
    ('ζ', r'\(\zeta\)'),
    ('η', r'\(\eta\)'),
    ('θ', r'\(\theta\)'),
    ('ι', r'\(\iota\)'),
    ('κ', r'\(\kappa\)'),
    ('λ', r'\(\lambda\)'),
    ('μ', r'\(\mu\)'),
    ('ν', r'\(\nu\)'),
    ('ξ', r'\(\xi\)'),
    ('ο', r'o'),
    ('π', r'\(\pi\)'),
    ('ρ', r'\(\rho\)'),
    ('σ', r'\(\sigma\)'),
    ('τ', r'\(\tau\)'),
    ('υ', '\\(\\upsilon\\)'),
    ('φ', r'\(\phi\)'),
    ('χ', r'\(\chi\)'),
    ('ψ', r'\(\psi\)'),
    ('ω', r'\(\omega\)'),
    ('Α', r'A'),
    ('Β', r'B'),
    ('Γ', r'\(\Gamma\)'),
    ('Δ', r'\(\Delta\)'),
    ('Ε', r'E'),
    ('Ζ', r'Z'),
    ('Η', r'H'),
    ('Θ', r'\(\Theta\)'),
    ('Ι', r'I'),
    ('Κ', r'K'),
    ('Λ', r'\(\Lambda\)'),
    ('Μ', r'M'),
    ('Ν', r'N'),
    ('Ξ', r'\(\Xi\)'),
    ('Ο', r'O'),
    ('Π', r'\(\Pi\)'),
    ('Ρ', r'P'),
    ('Σ', r'\(\Sigma\)'),
    ('Τ', r'T'),
    ('Υ', '\\(\\Upsilon\\)'),
    ('Φ', r'\(\Phi\)'),
    ('Χ', r'X'),
    ('Ψ', r'\(\Psi\)'),
    ('Ω', r'\(\Omega\)'),
    ('Ω', r'\(\Omega\)'),
]

tex_escape_map = {}

for a, b in tex_replacements:
    tex_escape_map[ord(a)] = b
